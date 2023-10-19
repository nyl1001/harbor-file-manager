package manager

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const (
	defaultHarborUserName     = "xxx"
	defaultHarborUserPassword = "xxxxx"
	defaultHarborProject      = "hub.xxxx.com/vmimages"
)

func TestMainThread(t *testing.T) {
	// 本地文件路径
	//localFilePath := "/home/q1/deploy.yml"
	localFileDir := "/tmp/harbor-file-uploader"
	err := createDirectorIfNotExist(localFileDir)
	if err != nil {
		return
	}
	localFilePath := filepath.Join(localFileDir, "test-file.json")

	// 生成 manifest 文件
	testFileContentJSON := []byte(`{
        "schemaVersion": 2,
		"mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:nieyinliang",
            "size": 12
        },
        "layers": [
			"mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:nieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliangnieyinliang",
            "size": 12
        ]
    }`)

	if err = createFile(localFilePath, testFileContentJSON); err != nil {
		return
	}

	// Harbor仓库地址和目标文件路径
	harborRepo := defaultHarborProject + "/nieyinliang-xx"
	harborTag := "latest"
	harborUsername := defaultHarborUserName
	harborPassword := defaultHarborUserPassword

	// 初始化容器引擎
	ctx := context.Background()

	hfM := SimpleNewOnce(harborUsername, harborPassword, defaultRootHarborCacheDir)
	err = hfM.CreateRepositoryIfNotExist(ctx, harborRepo, harborTag)
	if err != nil {
		fmt.Printf("Error hfM.CreateHarborRepositoryIfNotExist: %v\n", err)
		return
	}

	// 上传文件到Harbor

	blobInfo, err := hfM.UploadFile(ctx, localFilePath, harborRepo, harborTag)
	if err != nil {
		fmt.Printf("error hfM.UploadFile uploading file: %v\n", err)
		return
	}

	fmt.Println("file uploaded to Harbor successfully!")
	fmt.Println("blobInfo:", blobInfo)

	// 从Harbor下载文件
	//reader, _, err := hfM.GetDownloadReaderWithBlobDigest(ctx, harborRepo, harborTag, string(blobInfo.Digest))
	reader, _, err := hfM.GetDownloadReader(ctx, harborRepo, harborTag)
	if err != nil {

		fmt.Printf("Error downloading file: %v\n", err)
		return
	}

	defer func(reader io.ReadCloser) {
		err := reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建目录
	if err = os.MkdirAll("/tmp/images", os.ModePerm); err != nil {
		return
	}

	// 创建本地文件
	localFile, err := os.Create("/tmp/images/image")
	if err != nil {
		fmt.Printf("os.Create: %v\n", err)
		return
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		fmt.Printf("io.Copy: %v\n", err)
		return
	}

	fmt.Println("file downloaded from Harbor and cached locally successfully!")
}

// /Users/nieyinliang/work/vm-images/ubuntu:22.04-nvidia-535-cuda-11.img
func TestUploadVmImages(t *testing.T) {
	t.Parallel() // 允许并行执行
	// 本地文件路径
	localFilePath := "/Users/nieyinliang/work/vm-images/ubuntu:22.04-nvidia-535-cuda-11.img"

	// Harbor仓库地址和目标文件路径
	harborRepo := defaultHarborProject + "/ubuntu-22.04-nvidia-535-cuda-11.img"
	harborTag := "latest"
	harborUsername := defaultHarborUserName
	harborPassword := defaultHarborUserPassword

	// 初始化容器引擎
	ctx := context.Background()

	hfM := SimpleNewOnce(harborUsername, harborPassword, defaultRootHarborCacheDir)
	err := hfM.CreateRepositoryIfNotExist(ctx, harborRepo, harborTag)
	if err != nil {
		fmt.Printf("Error hfM.CreateHarborRepositoryIfNotExist: %v\n", err)
		return
	}

	// 上传文件到Harbor

	blobInfo, err := hfM.UploadFile(ctx, localFilePath, harborRepo, harborTag)
	if err != nil {
		fmt.Printf("error hfM.UploadFile uploading file: %v\n", err)
		return
	}

	fmt.Println("file uploaded to Harbor successfully!")
	fmt.Println("blobInfo:", blobInfo)

	// 从Harbor下载文件
	reader, _, err := hfM.GetDownloadReaderWithBlobDigest(ctx, harborRepo, harborTag, string(blobInfo.Digest))
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		return
	}

	defer func(reader io.ReadCloser) {
		err := reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建目录
	if err = os.MkdirAll("/tmp/images", os.ModePerm); err != nil {
		return
	}

	// 创建本地文件
	localFile, err := os.Create("/tmp/images/ubuntu:22.04-nvidia-535-cuda-11.img")
	if err != nil {
		fmt.Printf("os.Create: %v\n", err)
		return
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		fmt.Printf("io.Copy: %v\n", err)
		return
	}

	fmt.Println("file downloaded from Harbor and cached locally successfully!")
}

func TestDeleteRepo(t *testing.T) {

	harborUsername := defaultHarborUserName
	harborPassword := defaultHarborUserPassword

	harborRepo := defaultHarborProject + "/ubuntu-22.04-nvidia-535-cuda-11.img"
	// 初始化容器引擎

	hfM := SimpleNewOnce(harborUsername, harborPassword, defaultRootHarborCacheDir)
	err := hfM.DeleteRepo(context.Background(), harborRepo)
	if err != nil {
		fmt.Printf("error hfM.CreateHarborRepositoryIfNotExist: %v\n", err)
		return
	}

	fmt.Println("delete repo from Harbor successfully!")
}

// /Users/nieyinliang/work/vm-images/ubuntu:22.04-nvidia-535-cuda-11.img
func TestGetLayerDigest(t *testing.T) {
	// Harbor仓库地址和目标文件路径
	harborRepo := defaultHarborProject + "/ubuntu-22.04-nvidia-535-cuda-11.img"
	harborTag := "latest"

	harborUsername := defaultHarborUserName
	harborPassword := defaultHarborUserPassword

	// 初始化容器引擎
	ctx := context.Background()

	hfM := SimpleNewOnce(harborUsername, harborPassword, defaultRootHarborCacheDir)

	// 从Harbor下载文件
	digestStr, err := hfM.GetLatestLayerDigest(ctx, harborRepo, harborTag)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		return
	}

	fmt.Println(digestStr)
	fmt.Println("file downloaded from Harbor and cached locally successfully!")
}

// /Users/nieyinliang/work/vm-images/ubuntu:22.04-nvidia-535-cuda-11.img
func TestGetLatestArtifactDigest(t *testing.T) {
	// Harbor仓库地址和目标文件路径
	//harborRepo := defaultHarborProject + "/ubuntu-22.04-nvidia-535-cuda-11.img"
	harborRepo := "https://" + defaultHarborProject + "/ubuntu-22.04-nvidia-535-cuda-11.img"

	harborUsername := defaultHarborUserName
	harborPassword := defaultHarborUserPassword

	// 初始化容器引擎
	ctx := context.Background()

	hfM := SimpleNewOnce(harborUsername, harborPassword, defaultRootHarborCacheDir)

	// 从Harbor下载文件
	digestStr, err := hfM.GetLatestArtifactDigest(ctx, harborRepo)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		return
	}

	fmt.Println(digestStr)
	fmt.Println("file downloaded from Harbor and cached locally successfully!")
}
