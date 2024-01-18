package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	filePath  string
	chunkSize int
	method    string

	folderPath     string
	mergedFilePath string
)

func main() {
	flag.StringVar(&method, "method", "cut", "method")
	flag.StringVar(&filePath, "file", "", "file path")
	flag.IntVar(&chunkSize, "size", 0, "chunk size(Mb)")

	flag.StringVar(&folderPath, "folder", "", "folder path")
	flag.StringVar(&mergedFilePath, "merged", "", "merged file path")

	flag.Parse()

	if method == "cut" {
		err := cutFile(filePath, chunkSize)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		err := mergeFiles(folderPath, mergedFilePath)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func cutFile(filePath string, chunkSize int) error {
	if filePath == "" || chunkSize == 0 {
		flag.Usage()
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return err
	}

	// 计算切片数量
	fileSize := fileInfo.Size()
	chunkNumber := int(fileSize) / (chunkSize * 1024 * 1024)

	if int(fileSize)%(chunkSize*1024*1024) != 0 {
		chunkNumber++
	}

	// 获取文件名
	dir, fileName := filepath.Split(filePath)
	fileExt := filepath.Ext(fileName)
	fileName = fileName[:len(fileName)-len(fileExt)]

	timestamp := time.Now().Unix()
	// 创建切片
	for i := 0; i < chunkNumber; i++ {
		chunkFileName := fmt.Sprintf("%s-%d%s", fileName, i+1, fileExt)
		chunkFilePath := filepath.Join(dir, fmt.Sprintf("%s_%d", fileName, timestamp), chunkFileName)

		// 创建文件夹
		if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%s_%d", fileName, timestamp))); os.IsNotExist(err) {
			err := os.Mkdir(filepath.Join(dir, fmt.Sprintf("%s_%d", fileName, timestamp)), os.ModePerm)
			if err != nil {
				fmt.Println(err)
				return err
			}
		}

		err := CreateTrunk(chunkFileName, chunkFilePath, chunkSize, i, fileSize, file)
		// 如果创建切片失败，删除已经创建的切片
		if err != nil {
			for j := i; j >= 0; j-- {
				cleanChunks(fileName, fileExt, timestamp, dir, j)
			}

			log.Printf("Failed to create chunk %s\n", chunkFileName)
			break
		}
	}

	return nil
}

func CreateTrunk(chunkFileName string, chunkFilePath string, chunkSize int, index int, fileSize int64, file *os.File) error {
	// 创建切片文件
	chunkFile, err := os.Create(chunkFilePath)
	if err != nil {
		fmt.Println(err)
		return err
	}

	defer func(chunkFile *os.File) {
		_ = chunkFile.Close()
	}(chunkFile)

	chunkStart := int64(index) * int64(chunkSize*1024*1024)
	chunkEnd := chunkStart + int64(chunkSize*1024*1024)

	if chunkEnd > fileSize {
		chunkEnd = fileSize
	}

	_, err = file.Seek(chunkStart, io.SeekStart)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = io.CopyN(chunkFile, file, chunkEnd-chunkStart)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Created chunk %s with size %d\n", chunkFileName, chunkEnd-chunkStart)
	return nil
}

func cleanChunks(fileName string, fileExt string, timestamp int64, dir string, j int) {
	chunkFileName := fmt.Sprintf("%s-%d%s", fileName, j+1, fileExt)
	chunkFilePath := filepath.Join(dir, fmt.Sprintf("%s_%d", fileName, timestamp), chunkFileName)
	err := os.Remove(chunkFilePath)
	if err != nil {
		fmt.Println(err)
	}
}

// 合并文件
// folderPath: 切片文件路径
// mergedFilePath: 合并后的文件路径
func mergeFiles(folderPath string, mergedFilePath string) error {
	if folderPath == "" || mergedFilePath == "" {
		flag.Usage()
		return nil
	}

	mergedFile, err := os.Create(mergedFilePath)
	if err != nil {
		return err
	}
	defer func(mergedFile *os.File) {
		_ = mergedFile.Close()
	}(mergedFile)

	files, err := os.ReadDir(folderPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		err := copyFile(file, folderPath, mergedFile)
		if err != nil {
			log.Printf("Failed to merge file %s\n", file.Name())
			return err
		}
	}

	fmt.Printf("Merged files in %s to %s\n", folderPath, mergedFilePath)
	return nil
}

func copyFile(file os.DirEntry, folderPath string, mergedFile *os.File) error {
	if !file.IsDir() {
		filePath := filepath.Join(folderPath, file.Name())
		fmt.Printf("Merging file %s\n", filePath)

		chunkFile, err := os.Open(filePath)
		if err != nil {
			return err
		}
		defer func(chunkFile *os.File) {
			_ = chunkFile.Close()
		}(chunkFile)

		_, err = io.Copy(mergedFile, chunkFile)
		if err != nil {
			return err
		}
	}

	return nil
}
