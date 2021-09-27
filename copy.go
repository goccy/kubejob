package kubejob

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// CopyToPod copy directory or files to specified path on Pod.
func (e *JobExecutor) CopyToPod(srcPath, dstPath string) error {
	if len(srcPath) == 0 || len(dstPath) == 0 {
		return errCopyWithEmptyPath(srcPath, dstPath)
	}
	if _, err := os.Stat(srcPath); err != nil {
		return errCopy(srcPath, dstPath, fmt.Errorf("%s doesn't exist in local filesystem", srcPath))
	}

	// trim slash as the last character
	if dstPath != "/" && dstPath[len(dstPath)-1] == '/' {
		dstPath = dstPath[:len(dstPath)-1]
	}

	if _, err := e.exec([]string{"test", "-d", dstPath}); err == nil {
		// if dstPath is directory, copy specified src into it.
		dstPath = filepath.Join(dstPath, path.Base(srcPath))
	}

	tarCmd := []string{"tar", "--no-same-permissions", "--no-same-owner", "-xmf", "-"}
	dstDir := filepath.Dir(dstPath)
	if len(dstDir) > 0 {
		tarCmd = append(tarCmd, "-C", dstDir)
	}

	pod := e.Pod
	req := e.job.restClient.Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&core.PodExecOptions{
			Container: e.Container.Name,
			Command:   tarCmd,
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	url := req.URL()
	exec, err := remotecommand.NewSPDYExecutor(e.job.config, "POST", url)
	if err != nil {
		return fmt.Errorf("job: failed to create spdy executor: %w", err)
	}

	reader, writer := io.Pipe()

	var writerErr error
	go func() {
		defer writer.Close()
		writerErr = e.writeWithTar(writer, srcPath, dstPath)
	}()

	var (
		outCapturer bytes.Buffer
		errCapturer bytes.Buffer
	)
	readerErr := exec.Stream(remotecommand.StreamOptions{
		Stdin:  reader,
		Stdout: &outCapturer,
		Stderr: &errCapturer,
		Tty:    false,
	})
	if readerErr != nil || writerErr != nil {
		buf := []string{}
		stdout := outCapturer.String()
		if len(stdout) > 0 {
			buf = append(buf, stdout)
		}
		stderr := errCapturer.String()
		if len(stderr) > 0 {
			buf = append(buf, stderr)
		}
		return errCopyWithReaderWriter(srcPath, dstPath, readerErr, writerErr, strings.Join(buf, ":"))
	}
	return nil
}

// CopyFromPod copy directory or files from specified path on Pod.
func (e *JobExecutor) CopyFromPod(srcPath, dstPath string) error {
	if len(srcPath) == 0 || len(dstPath) == 0 {
		return errCopyWithEmptyPath(srcPath, dstPath)
	}

	pod := e.Pod
	req := e.job.restClient.Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&core.PodExecOptions{
			Container: e.Container.Name,
			Command:   []string{"tar", "cf", "-", srcPath},
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	url := req.URL()
	exec, err := remotecommand.NewSPDYExecutor(e.job.config, "POST", url)
	if err != nil {
		return fmt.Errorf("job: failed to create spdy executor: %w", err)
	}
	reader, writer := io.Pipe()

	var (
		writerErr   error
		errCapturer bytes.Buffer
	)
	go func() {
		defer func() {
			writer.Close()
		}()
		writerErr = exec.Stream(remotecommand.StreamOptions{
			Stdin:  nil,
			Stdout: writer,
			Stderr: &errCapturer,
			Tty:    false,
		})
	}()

	// tar trims the leading '/' if it's there
	tarPrefix := strings.TrimLeft(srcPath, "/")
	tarPrefix = e.trimShortcutPath(path.Clean(tarPrefix))
	readerErr := e.untarAll(reader, &errCapturer, tarPrefix, srcPath, dstPath)
	if readerErr != nil || writerErr != nil {
		return errCopyWithReaderWriter(srcPath, dstPath, readerErr, writerErr, errCapturer.String())
	}
	return nil
}

func (e *JobExecutor) trimShortcutPath(p string) string {
	const backPath = "../"

	newPath := path.Clean(p)
	trimmed := strings.TrimPrefix(newPath, backPath)

	for trimmed != newPath {
		newPath = trimmed
		trimmed = strings.TrimPrefix(newPath, backPath)
	}

	// trim leftover {".", ".."}
	if newPath == "." || newPath == ".." {
		newPath = ""
	}

	if len(newPath) > 0 && newPath[0] == '/' {
		return newPath[1:]
	}
	return newPath
}

func (e *JobExecutor) writeWithTar(w io.Writer, srcPath, dstPath string) error {
	writer := tar.NewWriter(w)
	defer writer.Close()

	srcPath = path.Clean(srcPath)
	dstPath = path.Clean(dstPath)
	if err := e.writeRecursiveWithTar(
		writer,
		path.Dir(srcPath),
		path.Base(srcPath),
		path.Dir(dstPath),
		path.Base(dstPath),
	); err != nil {
		return err
	}
	return nil
}

func (e *JobExecutor) writeRecursiveWithTar(w *tar.Writer, srcBase, srcFile, dstBase, dstFile string) error {
	srcPath := path.Join(srcBase, srcFile)
	matchedPaths, err := filepath.Glob(srcPath)
	if err != nil {
		return fmt.Errorf("failed to glob from %s: %w", srcPath, err)
	}
	for _, fpath := range matchedPaths {
		stat, err := os.Lstat(fpath)
		if err != nil {
			return fmt.Errorf("failed to lstat for %s: %w", fpath, err)
		}
		if stat.IsDir() {
			entries, err := os.ReadDir(fpath)
			if err != nil {
				return fmt.Errorf("failed to readdir %s: %w", fpath, err)
			}
			if len(entries) == 0 {
				hdr, _ := tar.FileInfoHeader(stat, fpath)
				hdr.Name = dstFile
				if err := w.WriteHeader(hdr); err != nil {
					return fmt.Errorf("failed to write header: %w", err)
				}
			}
			for _, entry := range entries {
				if err := e.writeRecursiveWithTar(
					w,
					srcBase,
					path.Join(srcFile, entry.Name()),
					dstBase,
					path.Join(dstFile, entry.Name()),
				); err != nil {
					return fmt.Errorf("failed to write recursive with tar for %s: %w", entry.Name(), err)
				}
			}
			return nil
		} else if stat.Mode()&os.ModeSymlink != 0 {
			// soft link
			hdr, _ := tar.FileInfoHeader(stat, fpath)
			target, err := os.Readlink(fpath)
			if err != nil {
				return fmt.Errorf("failed to readlink %s: %w", fpath, err)
			}

			hdr.Linkname = target
			hdr.Name = dstFile
			if err := w.WriteHeader(hdr); err != nil {
				return fmt.Errorf("failed to write header: %w", err)
			}
		} else {
			// regular file or other file type like pipe
			hdr, err := tar.FileInfoHeader(stat, fpath)
			if err != nil {
				return fmt.Errorf("failed to get header from %s: %w", fpath, err)
			}
			hdr.Name = dstFile

			if err := w.WriteHeader(hdr); err != nil {
				return fmt.Errorf("failed to write header: %w", err)
			}

			f, err := os.Open(fpath)
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", fpath, err)
			}
			defer f.Close()

			if _, err := io.Copy(w, f); err != nil {
				return fmt.Errorf("failed to copy %s: %w", fpath, err)
			}
			return nil
		}
	}
	return nil
}

func (e *JobExecutor) untarAll(r io.Reader, errCapturer io.Writer, prefix, srcPath, dstPath string) error {
	tarReader := tar.NewReader(r)
	for {
		header, err := tarReader.Next()
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("failed to get next header: %w", err)
			}
			break
		}

		// All the files will start with the prefix, which is the directory where
		// they were located on the pod, we need to strip down that prefix, but
		// if the prefix is missing it means the tar was tempered with.
		// For the case where prefix is empty we need to ensure that the path
		// is not absolute, which also indicates the tar file was tempered with.
		if !strings.HasPrefix(header.Name, prefix) {
			return fmt.Errorf("tar contents corrupted")
		}

		// basic file information
		mode := header.FileInfo().Mode()
		dstFileName := filepath.Join(dstPath, header.Name[len(prefix):])

		if !e.isDstRelative(dstPath, dstFileName) {
			fmt.Fprintf(errCapturer, "warning: file %q is outside target destination, skipping\n", dstFileName)
			continue
		}

		baseName := filepath.Dir(dstFileName)
		if err := os.MkdirAll(baseName, 0755); err != nil {
			return fmt.Errorf("failed to mkdir %s: %w", baseName, err)
		}
		if header.FileInfo().IsDir() {
			if err := os.MkdirAll(dstFileName, 0755); err != nil {
				return fmt.Errorf("failed to mkdir %s: %w", dstFileName, err)
			}
			continue
		}

		if mode&os.ModeSymlink != 0 {
			fmt.Fprintf(errCapturer, "warning: skipping symlink: %q -> %q\n", dstFileName, header.Linkname)
			continue
		}
		if err := e.copyFileFromReader(dstFileName, tarReader); err != nil {
			return fmt.Errorf("failed to copy file from reader %s: %w", dstFileName, err)
		}
	}

	return nil
}

func (e *JobExecutor) copyFileFromReader(file string, reader io.Reader) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, reader); err != nil {
		return err
	}
	return nil
}

// isDstRelative returns true if dest is pointing outside the base directory,
// false otherwise.
func (e *JobExecutor) isDstRelative(base, dst string) bool {
	relative, err := filepath.Rel(base, dst)
	if err != nil {
		return false
	}
	return relative == "." || relative == e.trimShortcutPath(relative)
}
