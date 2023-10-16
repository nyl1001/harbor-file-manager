package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Artifact struct {
	Accessories   interface{} `json:"accessories"`
	AdditionLinks struct {
		BuildHistory struct {
			Absolute bool   `json:"absolute"`
			Href     string `json:"href"`
		} `json:"build_history"`
		Vulnerabilities struct {
			Absolute bool   `json:"absolute"`
			Href     string `json:"href"`
		} `json:"vulnerabilities"`
	} `json:"addition_links"`
	Digest     string `json:"digest"`
	ExtraAttrs struct {
		Architecture string   `json:"architecture"`
		Author       string   `json:"author"`
		Config       struct{} `json:"config"`
		Created      string   `json:"created"`
		Os           string   `json:"os"`
	} `json:"extra_attrs"`
	Icon              string      `json:"icon"`
	ID                int         `json:"id"`
	Labels            interface{} `json:"labels"`
	ManifestMediaType string      `json:"manifest_media_type"`
	MediaType         string      `json:"media_type"`
	ProjectID         int         `json:"project_id"`
	PullTime          string      `json:"pull_time"`
	PushTime          string      `json:"push_time"`
	References        interface{} `json:"references"`
	RepositoryID      int         `json:"repository_id"`
	Size              int         `json:"size"`
	Tags              interface{} `json:"tags"`
	Type              string      `json:"type"`
}

func GetArtifactsByPage(_ context.Context, baseHarborUrl, projectName, repoName, harborUserName, harborUserPassword string, pageSize, page int) ([]Artifact, error) {
	harborReqURL := fmt.Sprintf(
		"%s/api/v2.0/projects/%s/repositories/%s/artifacts?with_tag=false&with_scan_overview=true&with_label=true&with_accessory=false&page_size=%d&page=%d",
		strings.TrimRight(baseHarborUrl, "/"), projectName, repoName, pageSize, page)

	fmt.Println(harborReqURL)

	client := &http.Client{}
	req, err := http.NewRequest("GET", harborReqURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(harborUserName, harborUserPassword)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to delete repo. Status code: %d, project name: %s, repo name: %s", resp.StatusCode, projectName, repoName)
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	var artifacts []Artifact
	err = json.NewDecoder(resp.Body).Decode(&artifacts)
	if err != nil {
		fmt.Printf("Error decoding JSON response: %v\n", err)
		return nil, err
	}

	return artifacts, nil
}

func GetLatestArtifactDigest(ctx context.Context, baseHarborUrl, projectName, repoName, harborUserName, harborUserPassword string) (string, error) {
	artifacts, err := GetArtifactsByPage(ctx, baseHarborUrl, projectName, repoName, harborUserName, harborUserPassword, 1, 1)
	if err != nil {
		return "", err
	}

	if len(artifacts) == 0 {
		return "", nil
	}

	// 找到具有最大 ID 值的 digest
	maxID := -1
	maxDigest := ""
	for _, artifact := range artifacts {
		if artifact.ID > maxID {
			maxID = artifact.ID
			maxDigest = artifact.Digest
		}
	}
	//maxDigest = strings.TrimPrefix(maxDigest, "sha256:")
	//fmt.Println(maxDigest)
	return maxDigest, nil
}

func DeleteHarborRepo(_ context.Context, baseHarborUrl, projectName, repoName, harborUserName, harborUserPassword string) error {
	repoAPI := strings.TrimRight(baseHarborUrl, "/") + "/api/v2.0/projects/" + projectName + "/repositories/" + repoName

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", repoAPI, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(harborUserName, harborUserPassword)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete repo. Status code: %d, project name: %s, repo name: %s", resp.StatusCode, projectName, repoName)
	}

	return nil
}
