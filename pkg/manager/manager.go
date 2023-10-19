package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/containers/image/v5/pkg/blobinfocache"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"
)

type FileManager interface {
	CreateRepositoryIfNotExist(ctx context.Context, harborRepo string, tag string) error
	UploadFile(ctx context.Context, localFilePath, harborRepo, tag string) (*types.BlobInfo, error)
	DownloadFile(ctx context.Context, harborRepo, tag string, targetFilePath string) error
	GetDownloadReader(ctx context.Context, harborRepo, tag string) (io.ReadCloser, int64, error)
	DownloadFileWithBlobDigest(ctx context.Context, harborRepo, tag, digestStr string, targetFilePath string) error
	GetDownloadReaderWithBlobDigest(ctx context.Context, harborRepo, tag, digestStr string) (io.ReadCloser, int64, error)
	DownloadFileWithBlob(ctx context.Context, harborRepo, tag, targetFilePath string, blobInfo *types.BlobInfo) error
	GetDownloadReaderWithBlob(ctx context.Context, harborRepo, tag string, blobInfo *types.BlobInfo) (io.ReadCloser, int64, error)
	DeleteImage(ctx context.Context, harborRepo, tag string) error
	DeleteRepo(ctx context.Context, harborRepo string) error
	GetLatestLayerDigest(ctx context.Context, harborRepo, tag string) (string, error)
	GetLatestArtifactDigest(ctx context.Context, harborRepo string) (string, error)
	GetBlobDigest(ctx context.Context, harborRepo, tag string) (string, error)
}

type fileManager struct {
	hifConf *FmConfig
}

type FmConfig struct {
	HarborUserName     string
	HarborUserPassword string
	RootCacheDir       string
}

var fmanager *fileManager

var fmOnce sync.Once

func SimpleNewOnce(harborUserName, harborUserPassword, rootCacheDir string) FileManager {
	fmOnce.Do(func() {
		fmanager = &fileManager{
			&FmConfig{
				HarborUserName:     harborUserName,
				HarborUserPassword: harborUserPassword,
				RootCacheDir:       rootCacheDir,
			},
		}
	})
	return fmanager
}

func NewOnce(config *FmConfig) FileManager {
	fmOnce.Do(func() {
		fmanager = &fileManager{
			config,
		}
	})
	return fmanager
}

func (fm *fileManager) CreateRepositoryIfNotExist(ctx context.Context, harborRepo, tag string) error {
	// 检查远程仓库是否已存在
	exists, err := checkRemoteRepoExists(ctx, fm.hifConf.HarborUserName, fm.hifConf.HarborUserPassword, harborRepo)
	if err != nil {
		return err
	}

	if !exists {
		_, ociImageName := extractProjectNameAndRepoName(harborRepo)
		ociImageDir := "/tmp/" + ociImageName
		err = createOCIImageLayout(ociImageDir)
		if err != nil {
			return err
		}
		// 上传第一个image，必要操作
		err = uploadLocalImageToHarbor(ctx, ociImageDir, fm.hifConf.HarborUserName, fm.hifConf.HarborUserPassword, harborRepo, tag)
		if err != nil {
			return err
		}
	}

	return nil
}

func (fm *fileManager) UploadFile(ctx context.Context, localFilePath, harborRepo, tag string) (*types.BlobInfo, error) {
	// 打开本地文件
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return nil, err
	}
	defer localFile.Close()

	// 准备上传的目标路径
	destRef := fmt.Sprintf("%s:%s", harborRepo, tag)

	// 使用 containers/image 库上传文件到Harbor
	imageRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s", destRef))
	if err != nil {
		return nil, err
	}

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir:                    fm.hifConf.RootCacheDir,
		DockerRegistryPushPrecomputeDigests: true,
	}

	destImg, err := imageRef.NewImageDestination(ctx, sys)
	if err != nil {
		return nil, err
	}

	// 获取文件信息
	fileInfo, err := localFile.Stat()
	if err != nil {
		return nil, err
	}
	// 获取文件大小
	fileSize := fileInfo.Size()

	// 使用 PutBlob 上传文件，并命中本地缓存， none.NoCache
	blobInfo, err := destImg.PutBlob(ctx, localFile, types.BlobInfo{Size: fileSize}, blobinfocache.DefaultCache(sys), false)
	if err != nil {
		return nil, err
	}

	err = updateManifest(ctx, imageRef, sys, &blobInfo, destImg, fileSize)
	if err != nil {
		return nil, err
	}

	return &blobInfo, nil
}

func updateManifest(ctx context.Context, imageRef types.ImageReference, sys *types.SystemContext, blobInfo *types.BlobInfo, destImg types.ImageDestination, fileSize int64) error {
	// Create an image source based on the reference
	imageSource, err := imageRef.NewImageSource(ctx, sys)
	if err != nil {
		return err
	}
	defer imageSource.Close()

	// Get the existing manifest
	originalManifest, _, err := imageSource.GetManifest(ctx, nil)
	if err != nil {
		return err
	}

	// Unmarshal the original manifest
	var manifest map[string]interface{}
	if err = json.Unmarshal([]byte(originalManifest), &manifest); err != nil {
		return err
	}

	// Create a new layer to add
	newLayer := map[string]interface{}{
		"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
		"digest":    blobInfo.Digest,
		"size":      fileSize,
	}

	// Append the new layer to the "layers" field in the manifest
	layers, exists := manifest["layers"].([]interface{})
	if exists {
		manifest["layers"] = append(layers, newLayer)
	} else {
		manifest["layers"] = []interface{}{newLayer}
	}

	// Marshal the updated manifest back to JSON
	updatedManifest, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	// Push the updated manifest back to the remote repository
	if err = destImg.PutManifest(ctx, updatedManifest, nil); err != nil {
		return err
	}
	return nil
}

func (fm *fileManager) GetBlobDigest(ctx context.Context, harborRepo, tag string) (string, error) {
	// 准备上传的目标路径
	destRef := fmt.Sprintf("%s:%s", harborRepo, tag)
	// 使用 containers/image 库上传文件到Harbor
	imageRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s", destRef))
	if err != nil {
		return "", err
	}

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir:                    fm.hifConf.RootCacheDir,
		DockerRegistryPushPrecomputeDigests: true,
	}

	// Create an image source based on the reference
	imageSource, err := imageRef.NewImageSource(ctx, sys)
	if err != nil {
		return "", err
	}

	latestDigest, err := getLatestLayerDigest(ctx, imageSource)
	if err != nil {
		return "", err
	}
	return latestDigest, nil
}

func getLatestLayerDigest(ctx context.Context, srcImg types.ImageSource) (string, error) {
	// Get the existing manifest
	originalManifest, _, err := srcImg.GetManifest(ctx, nil)
	if err != nil {
		return "", err
	}
	// 解析 JSON
	var manifestData map[string]interface{}
	if err = json.Unmarshal(originalManifest, &manifestData); err != nil {
		return "", err
	}

	// 提取 layers 数组
	layers, ok := manifestData["layers"].([]interface{})
	if !ok || len(layers) == 0 {
		return "", fmt.Errorf("no layers field found or it is not an array")
	}
	layerData, ok := layers[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid layer data at index 0")
	}
	digestStr, exists := layerData["digest"].(string)
	if !exists {
		return "", fmt.Errorf("no digest field found in layer at index 0")
	}
	return digestStr, nil
}

func (fm *fileManager) GetLatestLayerDigest(ctx context.Context, harborRepo, tag string) (string, error) {
	// 准备下载的源路径
	srcRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s:%s", harborRepo, tag))
	if err != nil {
		return "", err
	}
	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: fm.hifConf.RootCacheDir,
	}

	// 使用 image.NewImage 创建一个镜像对象
	srcImg, err := srcRef.NewImageSource(ctx, sys)
	if err != nil {
		return "", err
	}
	digestStr, err := getLatestLayerDigest(ctx, srcImg)
	if err != nil {
		return "", err
	}
	return digestStr, nil
}

func (fm *fileManager) GetLatestArtifactDigest(ctx context.Context, harborRepo string) (string, error) {
	harborHostname, projectName, repoName, err := parseHarborURL(harborRepo)
	if err != nil {
		return "", err
	}
	latestDigest, err := GetLatestArtifactDigest(ctx, "https://"+harborHostname, projectName, repoName, fm.hifConf.HarborUserName, fm.hifConf.HarborUserPassword)
	if err != nil {
		return "", err
	}
	return latestDigest, nil
}

func (fm *fileManager) GetDownloadReader(ctx context.Context, harborRepo, tag string) (io.ReadCloser, int64, error) {
	err := initRootCacheDir(fm.hifConf.RootCacheDir)
	if err != nil {
		return nil, 0, err
	}
	// 准备下载的源路径
	srcRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s:%s", harborRepo, tag))
	if err != nil {
		return nil, 0, err
	}
	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: fm.hifConf.RootCacheDir,
	}

	// 使用 image.NewImage 创建一个镜像对象
	srcImg, err := srcRef.NewImageSource(ctx, sys)
	if err != nil {
		return nil, 0, err
	}

	latestDigest, err := getLatestLayerDigest(ctx, srcImg)
	if err != nil {
		return nil, 0, err
	}
	// 获取文件内容，并检查并命中本地缓存
	reader, size, err := srcImg.GetBlob(ctx, types.BlobInfo{
		Digest:               digest.Digest(latestDigest),
		Size:                 0,
		URLs:                 nil,
		Annotations:          nil,
		MediaType:            "",
		CompressionOperation: 0,
		CompressionAlgorithm: nil,
		CryptoOperation:      0,
	}, blobinfocache.DefaultCache(sys))
	if err != nil {
		return nil, 0, err
	}
	return reader, size, nil
}

func (fm *fileManager) DownloadFile(ctx context.Context, harborRepo, tag, targetFilePath string) error {
	// 从Harbor下载文件
	reader, _, err := fm.GetDownloadReader(ctx, harborRepo, tag)
	if err != nil {
		return err
	}

	defer func(reader io.ReadCloser) {
		err = reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建本地文件
	localFile, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		return err
	}
	return nil
}

func (fm *fileManager) GetDownloadReaderWithBlobDigest(ctx context.Context, harborRepo, tag, digestStr string) (io.ReadCloser, int64, error) {
	err := initRootCacheDir(fm.hifConf.RootCacheDir)
	if err != nil {
		return nil, 0, err
	}
	// 准备下载的源路径
	srcRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s:%s", harborRepo, tag))
	if err != nil {
		return nil, 0, err
	}
	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: fm.hifConf.RootCacheDir,
	}

	// 使用 image.NewImage 创建一个镜像对象
	srcImg, err := srcRef.NewImageSource(ctx, sys)
	if err != nil {
		return nil, 0, err
	}

	// 获取文件内容，并检查并命中本地缓存
	reader, size, err := srcImg.GetBlob(ctx, types.BlobInfo{
		Digest:               digest.Digest(digestStr),
		Size:                 0,
		URLs:                 nil,
		Annotations:          nil,
		MediaType:            "",
		CompressionOperation: 0,
		CompressionAlgorithm: nil,
		CryptoOperation:      0,
	}, blobinfocache.DefaultCache(sys))
	if err != nil {
		return nil, 0, err
	}
	return reader, size, nil
}

func (fm *fileManager) DownloadFileWithBlobDigest(ctx context.Context, harborRepo, tag, digestStr, targetFilePath string) error {
	// 从Harbor下载文件
	reader, _, err := fm.GetDownloadReaderWithBlobDigest(ctx, harborRepo, tag, digestStr)
	if err != nil {
		return err
	}

	defer func(reader io.ReadCloser) {
		err = reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建本地文件
	localFile, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		return err
	}
	return nil
}

func (fm *fileManager) GetDownloadReaderWithBlob(ctx context.Context, harborRepo, tag string, blobInfo *types.BlobInfo) (io.ReadCloser, int64, error) {
	err := initRootCacheDir(fm.hifConf.RootCacheDir)
	if err != nil {
		return nil, 0, err
	}
	// 准备下载的源路径
	srcRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s:%s", harborRepo, tag))
	if err != nil {
		return nil, 0, err
	}
	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: fm.hifConf.RootCacheDir,
	}

	// 使用 image.NewImage 创建一个镜像对象
	srcImg, err := srcRef.NewImageSource(ctx, sys)
	if err != nil {
		return nil, 0, err
	}

	// 获取文件内容，并检查并命中本地缓存
	reader, size, err := srcImg.GetBlob(ctx, *blobInfo, blobinfocache.DefaultCache(sys))
	if err != nil {
		return nil, 0, err
	}
	return reader, size, nil
}

func (fm *fileManager) DownloadFileWithBlob(ctx context.Context, harborRepo, tag, targetFilePath string, blobInfo *types.BlobInfo) error {
	// 从Harbor下载文件
	reader, _, err := fm.GetDownloadReaderWithBlob(ctx, harborRepo, tag, blobInfo)
	if err != nil {
		return err
	}

	defer func(reader io.ReadCloser) {
		err = reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建本地文件
	localFile, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		return err
	}
	return nil
}

func (fm *fileManager) DeleteImage(ctx context.Context, harborRepo, tag string) error {

	// 准备上传的目标路径
	destRef := fmt.Sprintf("%s:%s", harborRepo, tag)

	// 使用 containers/image 库上传文件到Harbor
	destCtx, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s", destRef))
	if err != nil {
		return err
	}

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: fm.hifConf.HarborUserName,
			Password: fm.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: fm.hifConf.RootCacheDir,
	}

	err = destCtx.DeleteImage(ctx, sys)
	if err != nil {
		return err
	}
	return nil
}

func (fm *fileManager) DeleteRepo(ctx context.Context, harborRepo string) error {
	// 构建项目 API URL
	harborHostname, projectName, repoName, err := parseHarborURL(harborRepo)
	if err != nil {
		return err
	}
	err = DeleteHarborRepo(ctx, "https://"+harborHostname, projectName, repoName, fm.hifConf.HarborUserName, fm.hifConf.HarborUserPassword)
	if err != nil {
		return err
	}
	return nil
}
