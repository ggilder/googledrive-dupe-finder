package main

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/api/drive/v3"

	"github.com/rafaeljesus/retry-go"
)

type DriveListing struct {
	service      *drive.Service
	RootPath     string
	rootId       string
	driveFiles   []*drive.File
	driveFolders map[string]*googleDriveFolder
}

type googleDriveFolder struct {
	ParentId, Name, path string
}

type folderNotFoundError struct {
	id string
}

func (e folderNotFoundError) Error() string {
	return fmt.Sprintf("Folder id %s not found", e.id)
}

func NewDriveListing(service *drive.Service) *DriveListing {
	inst := &DriveListing{}
	inst.service = service
	inst.RootPath = "/"
	return inst
}

func (g *DriveListing) Files(updateChan chan<- int) (files []*File, err error) {
	scannedFiles := 0
	nextPageToken := ""
	g.driveFiles = []*drive.File{}
	g.driveFolders = make(map[string]*googleDriveFolder)
	g.rootId, err = g.getRootId()
	if err != nil {
		return
	}
	g.driveFolders[g.rootId] = &googleDriveFolder{path: "/"}

	for {
		result, err := g.listAll(nextPageToken)
		if err != nil {
			return nil, err
		}

		nextPageToken = result.NextPageToken
		scannedFiles += g.handleDriveFiles(result.Files)
		updateChan <- scannedFiles

		if nextPageToken == "" {
			break
		}
	}

	for _, file := range g.driveFiles {
		parentId := g.rootId
		if len(file.Parents) > 0 {
			parentId = file.Parents[0]
		}
		parentPath, err := g.buildPath(parentId)
		if err != nil {
			switch err := err.(type) {
			case folderNotFoundError:
				// skip file - this indicates it's in a shared folder owned by someone else, which doesn't sync locally
				continue
			default:
				return nil, err
			}
		}
		relPath, err := filepath.Rel(g.RootPath, path.Join(parentPath, file.Name))
		if err != nil {
			return nil, err
		}
		// filter files outside of the specified root
		if !strings.HasPrefix(relPath, "../") {
			normalizedPath := strings.ToLower(normalizePath(relPath))
			files = append(files, &File{Path: normalizedPath, ContentHash: file.Md5Checksum, Size: file.Size})
		}
	}
	return
}

const apiRetries int = 10

func (g *DriveListing) listAll(nextPageToken string) (result *drive.FileList, err error) {
	err = retry.Do(func() error {
		result, err = g.service.Files.List().
			PageToken(nextPageToken).
			PageSize(1000).
			Fields("nextPageToken, files(id, name, parents, ownedByMe, trashed, md5Checksum, mimeType, size)").
			Q("trashed != true").
			Do()
		return err
	}, apiRetries, time.Second*1)
	return
}

func (g *DriveListing) getRootId() (string, error) {
	var file *drive.File
	var err error
	err = retry.Do(func() error {
		file, err = g.service.Files.Get("root").Fields("id").Do()
		return err
	}, apiRetries, time.Second*1)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Unable to retrieve root: %v", err))
	} else {
		return file.Id, nil
	}
}

func (g *DriveListing) handleDriveFiles(files []*drive.File) int {
	handledFiles := 0
	for _, file := range files {
		var parentId string
		if len(file.Parents) == 0 {
			// parentId = g.rootId
			// ignore files without parent
			continue
		} else {
			parentId = file.Parents[0]
		}
		if file.MimeType == "application/vnd.google-apps.folder" {
			g.driveFolders[file.Id] = &googleDriveFolder{
				ParentId: parentId,
				Name:     file.Name,
			}
		} else if file.Md5Checksum != "" {
			g.driveFiles = append(g.driveFiles, file)
			handledFiles++
		}
	}
	return handledFiles
}

func (g *DriveListing) buildPath(folderId string) (string, error) {
	if folder, ok := g.driveFolders[folderId]; ok {
		if folder.path == "" {
			parentPath, err := g.buildPath(folder.ParentId)
			if err != nil {
				return "", err
			}
			folder.path = path.Join(parentPath, folder.Name)
		}
		return folder.path, nil
	} else {
		return "", folderNotFoundError{id: folderId}
	}
}
