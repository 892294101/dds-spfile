package ddsspfile

import (
	"fmt"
	"github.com/892294101/dds/utils"
	"github.com/sirupsen/logrus"
	"regexp"
	"strings"
)

const (
	IpV4Reg  = "^((0|[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-5])\\.){3}(0|[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-5])$"
	IpV4Port = "^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$"
	/*	SourceDBRegular = "(^)" +
		"(?i:(" + SourceDBType + "))  (\\s+) (((0|[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-5])\\.){3}(0|[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-5])) (\\s+)" +
		"(?i:(" + Port + ")) (\\s+) (\\d+) (\\s+)" +
		"(?i:(" + DataBase + ")) (*) (\\s+) " +
		"(?i:(" + Types + ")) (" + MySQL + "|" + "MariaDB" + ") (\\s+) " +
		"(?i:(" + UserId + ")) (*) (\\s+) " +
		"(?i:(" + PassWord + ")) (*) (\\s+) " +
		"($)"*/
)

type ownerTable struct {
	ownerValue string
	tableValue string
}

type ETL struct {
	addColumn    string
	deleteColumn string
	updateColumn string
	mapColumn    string
}

func GetMySQLName() string {
	return utils.MySQL
}

func GetOracleName() string {
	return utils.Oracle
}

func GetExtractName() string {
	return utils.Extract
}

func GetReplicationName() string {
	return utils.Replicat
}

func ValToUper(v string) string {
	if strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) {
		return strings.Trim(v, `"`)
	}
	return strings.ToUpper(strings.Trim(v, `"`))
}

func MatchSchemaTable(owner, table, val *string, log *logrus.Logger) bool {
	switch {
	case strings.HasSuffix(*val, "*") && !strings.HasPrefix(*val, "*"): // 检查*号是否在结尾,头部不可以有星号
		re, err := regexp.Compile(fmt.Sprintf("^(%v)", strings.TrimRight(*val, "*")))
		if err != nil {
			log.Warnf("schema and user(%v.%v) regularization failed: %v", *owner, *table, err)
			return false
		}
		if re.MatchString(*table) {
			return true
		}
	case strings.HasPrefix(*val, "*") && !strings.HasSuffix(*val, "*"): // 检查*号是否在头部,结尾不可以有星号
		re, err := regexp.Compile(fmt.Sprintf("(%v)$", strings.TrimLeft(*val, "*")))
		if err != nil {
			log.Warnf("schema and user(%v.%v) regularization failed: %v", *owner, *table, err)
			return false
		}
		if re.MatchString(*table) {
			return true
		}
	case strings.HasPrefix(*val, "*") && strings.HasSuffix(*val, "*"):
	}
	return false
}

type Module interface {
	Init()
	Add(raw *string) error
	ListParamText() string
	MarshalJson() ([]byte, error)
	GetParam() interface{}
}

type Parameter interface {
	put() string
	init()
	add(raw *string) error
	initDefault() error
	isType(raw *string, dbType *string, processType *string) error
	parse(raw *string) error
}

type Parameters interface {
	Module
	Registry() map[string]Parameter
}
