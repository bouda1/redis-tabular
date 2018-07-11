package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "regexp"
    "strings"
)

// MaxDepth Max depth in search tree
const MaxDepth = 2

func findIncludes(file string, treated *map[string]bool, depth int) {
    var myList []string
    file1 := file
    f, err := os.Open(file1)
    if err != nil {
        file1 = strings.TrimPrefix(file, "src/")
        for _, pref := range []string{"/usr/local/include/", "usr/include/"} {
            f, err = os.Open(pref + file1)
            if err == nil {
                file1 = pref + file1
                if !(*treated)[file1] {
                    (*treated)[file1] = true
                }
                break
            }
        }
    } else {
        (*treated)[file1] = true
    }
    defer f.Close()

    depth++
    if depth > MaxDepth {
        return
    }

    scanner := bufio.NewScanner(f)
    r, _ := regexp.Compile("^#\\s*include\\s*([<\"])(.*)[>\"]")
    for scanner.Scan() {
        line := scanner.Text()
        match := r.FindStringSubmatch(line)
        if len(match) > 0 {
            /* match[0] is the global match, match[1] is '<' or '"' and match[2] is the file to include */
            if match[1] == "\"" {
                fmt.Printf("  \"%s\" -> \"%s\";\n", file1, "src/"+match[2])
                myList = append(myList, "src/"+match[2])
            } else {
                fmt.Printf("  \"%s\" -> \"%s\";\n", file1, match[2])
            }
        }
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

    for _, file2 := range myList {
        if !(*treated)[file2] {
            findIncludes(file2, treated, depth)
        }
    }
}

func main() {
    args := os.Args[1:]
    var fileList []string

    if len(args) == 0 {
        for _, searchDir := range []string{"src"} {
            filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
                if strings.HasSuffix(path, ".c") || strings.HasSuffix(path, ".h") {
                    fileList = append(fileList, path)
                }
                return nil
            })
        }
    } else {
        fileList = append(fileList, args[0])
    }

    fmt.Println("digraph graphname {")

    var treated = make(map[string]bool)
    for _, file := range fileList {
        if !treated[file] {
            findIncludes(file, &treated, 0)
        }
    }
    fmt.Println("}")
}
