package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/directory"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
)

const defaultRootHarborCacheDir = "/var/lib/harbor-file-cache/"

func createOCIImageLayout(ociImageDir string) error {
	err := createDirectorIfNotExist(ociImageDir)
	if err != nil {
		return err
	}
	// 创建OCI镜像目录结构
	blobsDir := filepath.Join(ociImageDir, "blobs")
	if err = os.MkdirAll(blobsDir, os.ModePerm); err != nil {
		return err
	}

	blobsShaDir := filepath.Join(blobsDir, "sha256")
	if err = os.MkdirAll(blobsShaDir, os.ModePerm); err != nil {
		return err
	}
	// 创建空的 config 文件
	configContent := []byte(`{
		"user": "1000:1000",
		"Cmd": ["echo", "Hello, OCI Image!"]
	}`)
	configDigest, err := createDigestFile(blobsShaDir, configContent)
	if err != nil {
		return err
	}

	if err = createFile(filepath.Join(ociImageDir, configDigest), configContent); err != nil {
		return err
	}

	// 生成 manifest 文件
	manifestJSON := []byte(fmt.Sprintf(`{
        "schemaVersion": 2,
		"mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:%s",
            "size": %d
        },
        "layers": [
        ]
    }`, configDigest, len(configContent)))

	if err = createFile(filepath.Join(ociImageDir, "manifest.json"), manifestJSON); err != nil {
		return err
	}

	// 创建config.json示例
	configJSON := []byte(fmt.Sprintf(`{
		"mediaType": "application/vnd.oci.image.config.v1+json",
		"digest": "%s",
		"size": %d,
		"config": {
			"user": "1000:1000",
			"Cmd": ["echo", "Hello, OCI Image!"]
		}
	}`, configDigest, len(string(configContent))))

	if err = createFile(filepath.Join(blobsDir, "sha256", configDigest), configJSON); err != nil {
		return err
	}

	return nil
}

func createFile(filePath string, content []byte) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		return err
	}

	return nil
}

func createDigestFile(directory string, content []byte) (string, error) {
	digest := sha256.Sum256(content)
	digestStr := hex.EncodeToString(digest[:])
	filePath := directory + "/" + digestStr

	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		return "", err
	}

	return digestStr, nil
}

// 检查远程 Harbor 仓库是否已存在
func checkRemoteRepoExists(ctx context.Context, harborUsername, harborPassword, harborRepo string) (bool, error) {
	harborImage := fmt.Sprintf("docker://%s", harborRepo)

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: harborUsername,
			Password: harborPassword,
		},
	}

	// 解析仓库地址
	refCtx, err := alltransports.ParseImageName(harborImage)
	if err != nil {
		return false, fmt.Errorf("error checkRemoteRepoExists call alltransports.ParseImageName: %s", err.Error())
	}

	// 获取远程镜像
	_, err = refCtx.NewImageSource(ctx, sys)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, fmt.Errorf("error checkRemoteRepoExists call checkRemoteRepoExists NewImageSource: %s", err.Error())
	}

	// 图像存在于本地
	return true, nil
}

// 上传 OCI 镜像目录到远程仓库
func uploadLocalImageToHarbor(ctx context.Context, imageDirectory, harborUsername, harborPassword, harborRepo, harborTag string) error {
	// 使用github.com/containers/image库上传镜像到Harbor
	harborImage := fmt.Sprintf("docker://%s:%s", harborRepo, harborTag)

	// 创建一个简单的默认策略
	defaultPolicy := `{
		"default": [
			{
				"type": "insecureAcceptAnything"
			}
		]
	}`

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: harborUsername,
			Password: harborPassword,
		},
	}

	// 创建一个签名策略
	policy, err := signature.NewPolicyFromBytes([]byte(defaultPolicy))
	if err != nil {
		return fmt.Errorf("error uploadLocalImageToHarbor call signature.NewPolicyFromBytes, can not create signature policy: %s", err.Error())
	}

	policyContext, err := signature.NewPolicyContext(policy)
	if err != nil {
		return fmt.Errorf("error uploadLocalImageToHarbor call signature.NewPolicyContext: %s", err.Error())
	}

	destCtx, err := alltransports.ParseImageName(harborImage)
	if err != nil {
		return fmt.Errorf("error uploadLocalImageToHarbor call alltransports.ParseImageName: %s", err.Error())
	}

	srcCtx, err := directory.NewReference(imageDirectory)
	if err != nil {
		return fmt.Errorf("error uploadLocalImageToHarbor call directory.NewReference: %s", err.Error())
	}

	_, err = copy.Image(ctx, policyContext, destCtx, srcCtx, &copy.Options{
		ReportWriter:   nil, // 如果需要的话，可以提供一个报告写入器
		DestinationCtx: sys,
		SourceCtx:      sys,
	})
	if err != nil {
		return fmt.Errorf("error uploadLocalImageToHarbor copy.Image: %s", err.Error())
	}

	return nil
}

func initRootCacheDir(cacheDir string) error {
	if cacheDir == "" {
		cacheDir = defaultRootHarborCacheDir
	}
	return createDirectorIfNotExist(cacheDir)
}

func createDirectorIfNotExist(directoryPath string) error {
	// 检查目录是否存在
	_, err := os.Stat(directoryPath)

	// 如果目录不存在，则创建
	if os.IsNotExist(err) {
		err = os.MkdirAll(directoryPath, os.ModePerm)
		return err
	}
	return err
}

func extractProjectNameAndRepoName(repoPathURI string) (string, string) {
	// 使用字符串分割函数来获取最后一个部分
	repoPathURI = strings.Trim(repoPathURI, "/")
	mainParts := strings.Split(repoPathURI, "/")
	if len(mainParts) == 0 {
		return "", ""
	}

	projectName := mainParts[0]
	lastMainPart := mainParts[len(mainParts)-1]
	if !strings.Contains(lastMainPart, ":") {
		return projectName, lastMainPart
	}

	repoName := ""
	subParts := strings.Split(lastMainPart, ":")
	if len(subParts) > 2 {
		imageNameParts := subParts[:len(subParts)-1]
		repoName = strings.Join(imageNameParts, "-")
	} else if len(subParts) == 1 {
		repoName = subParts[0]
	} else {
		repoName = ""
	}

	return projectName, repoName
}

func extractHostnameAndPathFromURL(inputURL string) (string, string, error) {
	inputURL = strings.Trim(inputURL, " ")
	if !strings.HasPrefix(inputURL, "http") {
		inputURLArray := strings.Split(inputURL, "/")
		return inputURLArray[0], strings.TrimPrefix(inputURL, inputURLArray[0]), nil
	}
	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		return "", "", err
	}
	hostname := parsedURL.Hostname()
	path := parsedURL.Path
	return hostname, path, nil
}

func parseHarborURL(harborRepoURL string) (string, string, string, error) {
	harborHostname, uriPath, err := extractHostnameAndPathFromURL(harborRepoURL)
	if err != nil {
		return "", "", "", err
	}
	projectName, repoName := extractProjectNameAndRepoName(uriPath)
	if err != nil {
		return "", "", "", err
	}
	return harborHostname, projectName, repoName, nil
}
