package ddsspfile

import (
	"fmt"
	"github.com/892294101/dds-utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

type Encoding uint

const (
	utf8Default Encoding = iota
	UTF8
	ISO88591
)

type spfileBaseInfo struct {
	enc         Encoding //文件字符集
	file        string   //文件
	log         *logrus.Logger
	dbType      string
	processType string
	homeDir     string
}

func LoadSpfile(file string, enc Encoding, log *logrus.Logger, dbType string, processType string) (*Spfile, error) {
	if len(file) == 0 {
		return nil, errors.New(fmt.Sprintf("Parameter file path must be specified"))
	}

	home, err := ddsutils.GetHomeDirectory()
	if err != nil {
		return nil, err
	}
	fh := new(spfileBaseInfo)
	fh.enc = enc
	fh.file = filepath.Join(*home, "param", file)
	fh.log = log
	fh.dbType = dbType
	fh.processType = processType
	fh.homeDir = *home
	return fh.LoadFile(fh)
}

func (f *spfileBaseInfo) LoadFile(fh *spfileBaseInfo) (*Spfile, error) {
	_, err := os.Stat(fh.file)
	if os.IsNotExist(err) {
		return nil, errors.Errorf("File not found: %s", fh.file)
	}
	return &Spfile{paramBaseInfo: fh, log: fh.log}, nil
}
