package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func main() {
	var (
		filePath  string
		chunkSize int
	)

	flag.StringVar(&filePath, "file", "", "file path")
	flag.IntVar(&chunkSize, "size", 0, "chunk size(Mb)")
	flag.Parse()

	if filePath == "" || chunkSize == 0 {
		flag.Usage()
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	fileSize := fileInfo.Size()
	chunkNumber := int(fileSize) / (chunkSize * 1024 * 1024)

	if int(fileSize)%(chunkSize*1024*1024) != 0 {
		chunkNumber++
	}

	dir, fileName := filepath.Split(filePath)
	fileExt := filepath.Ext(fileName)
	fileName = fileName[:len(fileName)-len(fileExt)]

	for i := 0; i < chunkNumber; i++ {
		chunkFileName := fmt.Sprintf("%s-%d%s", fileName, i+1, fileExt)
		chunkFilePath := filepath.Join(dir, chunkFileName)

		chunkFile, err := os.Create(chunkFilePath)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer chunkFile.Close()

		chunkStart := int64(i) * int64(chunkSize*1024*1024)
		chunkEnd := chunkStart + int64(chunkSize*1024*1024)

		if chunkEnd > fileSize {
			chunkEnd = fileSize
		}

		_, err = file.Seek(chunkStart, io.SeekStart)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = io.CopyN(chunkFile, file, chunkEnd-chunkStart)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("Created chunk %s with size %d\n", chunkFileName, chunkEnd-chunkStart)
	}
}

func mergeFiles(folderPath string, mergedFilePath string) error {
	mergedFile, err := os.Create(mergedFilePath)
	if err != nil {
		return err
	}
	defer mergedFile.Close()

	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(folderPath, file.Name())
			fmt.Printf("Merging file %s\n", filePath)

			chunkFile, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer chunkFile.Close()

			_, err = io.Copy(mergedFile, chunkFile)
			if err != nil {
				return err
			}
		}
	}

	fmt.Printf("Merged files in %s to %s\n", folderPath, mergedFilePath)
	return nil
}
