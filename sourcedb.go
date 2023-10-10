package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds/utils"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type PortModel struct {
	key   *string
	value *uint16
}

type TypeModel struct {
	key   *string
	value *string
}

type UserIdModel struct {
	key   *string
	value *string
}

type PassWordModel struct {
	key   *string
	value *string
}

type ServerIdModel struct {
	key   *string
	value *uint32
}

type RetryMaxConnect struct {
	key   *string
	value *int
}

type ClientCharacterSet struct {
	key   *string
	value *string
}

type ClientCollation struct {
	key   *string
	value *string
}

type TimeZone struct {
	key   *string
	value *time.Location
}

type dbInfo struct {
	address            *string             // 数据库地址
	port               *PortModel          // 数据库端口
	types              *TypeModel          // 连接数据库类型, mysql或 mariadb
	userId             *UserIdModel        // 用户名
	passWord           *PassWordModel      // 密码
	serverId           *ServerIdModel      // mysql server id
	retryMaxConnNumber *RetryMaxConnect    // 连接重连最大次数
	clientCharacter    *ClientCharacterSet // 客户端字符集
	clientCollation    *ClientCollation    // 客户端字符集
	timeZone           *TimeZone
}

func (d *dbInfo) GetAddress() *string         { return d.address }
func (d *dbInfo) GetPort() *uint16            { return d.port.value }
func (d *dbInfo) GetTypes() *string           { return d.types.value }
func (d *dbInfo) GetUserId() *string          { return d.userId.value }
func (d *dbInfo) GetPassWord() *string        { return d.passWord.value }
func (d *dbInfo) GetServerID() *uint32        { return d.serverId.value }
func (d *dbInfo) GetRetryConnect() *int       { return d.retryMaxConnNumber.value }
func (d *dbInfo) GetClientCharacter() *string { return d.clientCharacter.value }
func (d *dbInfo) GetClientCollation() *string { return d.clientCollation.value }
func (d *dbInfo) GetTimeZone() *time.Location { return d.timeZone.value }
func (d *dbInfo) GetConnInfo() *dbInfo        { return d }

type SourceDB struct {
	supportParams map[string]map[string]string // 参数支持吃数据库和进程
	paramPrefix   *string                      // 参数前缀
	DBInfo        *dbInfo
}

func (s *SourceDB) put() string {
	return fmt.Sprintf("%s %s %s %d %s %s %s %s %s %s %s %d %s %s %s %s %s %s\n",
		*s.paramPrefix,
		*s.DBInfo.GetAddress(),
		*s.DBInfo.port.key,
		*s.DBInfo.GetPort(),
		*s.DBInfo.types.key,
		*s.DBInfo.GetTypes(),
		*s.DBInfo.userId.key,
		*s.DBInfo.GetUserId(),
		*s.DBInfo.passWord.key,
		*s.DBInfo.GetPassWord(),
		*s.DBInfo.serverId.key,
		*s.DBInfo.GetServerID(),
		*s.DBInfo.timeZone.key,
		s.DBInfo.GetTimeZone().String(),
		*s.DBInfo.clientCharacter.key,
		*s.DBInfo.GetClientCharacter(),
		*s.DBInfo.clientCollation.key,
		*s.DBInfo.GetClientCollation(),
	)
}

// 初始化参数可以支持的数据库和进程

func (s *SourceDB) init() {
	s.supportParams = map[string]map[string]string{
		utils.MySQL: {
			utils.Extract: utils.Extract,
		},
	}
	/*s.Port = new(PortModel)
	s.Database = new(DatabaseModel)
	s.Type = new(TypeModel)
	s.UserId = new(UserIdModel)
	s.PassWord = new(PassWordModel)
	*/
}

func (s *SourceDB) initDefault() error {
	return nil
}

func (s *SourceDB) isType(raw *string, dbType *string, processType *string) error {
	s.init()
	_, ok := s.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (s *SourceDB) parse(raw *string) error {
	sdb := utils.TrimKeySpace(strings.Split(*raw, " "))
	sdbLength := len(sdb) - 1

	for i := 0; i < len(sdb); i++ {
		switch {
		case strings.EqualFold(sdb[i], utils.SourceDBType):
			s.paramPrefix = &sdb[i]
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.SourceDBType)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.address != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			match, _ := regexp.MatchString(IpV4Reg, *NextVal)
			if !match {
				return errors.Errorf("%s is an illegal IPV4 address\n", *NextVal)
			}

			s.DBInfo.address = NextVal
			i += 1
		case strings.EqualFold(sdb[i], utils.Port):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.Port)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.port != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			match, _ := regexp.MatchString(IpV4Port, *NextVal)
			if !match {
				return errors.Errorf("%s is an illegal IPV4 Port\n", *NextVal)
			}

			p, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s Port conversion failed", *NextVal)
			}
			port := uint16(p)
			s.DBInfo.port = &PortModel{key: &sdb[i], value: &port}
			i += 1
		case strings.EqualFold(sdb[i], utils.Types):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.Types)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.types != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s.DBInfo.types = &TypeModel{key: &sdb[i], value: NextVal}
			i += 1
		case strings.EqualFold(sdb[i], utils.UserId):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.UserId)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.userId != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s.DBInfo.userId = &UserIdModel{key: &sdb[i], value: NextVal}
			i += 1
		case strings.EqualFold(sdb[i], utils.PassWord):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.PassWord)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.passWord != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s.DBInfo.passWord = &PassWordModel{key: &sdb[i], value: NextVal}
			i += 1
		case strings.EqualFold(sdb[i], utils.ServerId):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.ServerId)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.serverId != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			p, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s server id conversion failed", *NextVal)
			}
			id := uint32(p)
			s.DBInfo.serverId = &ServerIdModel{key: &sdb[i], value: &id}
			i += 1
		case strings.EqualFold(sdb[i], utils.Retry):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.Retry)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.retryMaxConnNumber != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			retryNum, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s %s conversion failed", *NextVal, utils.Retry)
			}

			if retryNum > 3 && retryNum < 12 {
				s.DBInfo.retryMaxConnNumber = &RetryMaxConnect{key: &sdb[i], value: &retryNum}
			}
			i += 1
		case strings.EqualFold(sdb[i], utils.Character):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.Character)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.clientCharacter != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s.DBInfo.clientCharacter = &ClientCharacterSet{key: &sdb[i], value: NextVal}
			i += 1
		case strings.EqualFold(sdb[i], utils.Collation):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.Collation)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.clientCollation != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s.DBInfo.clientCollation = &ClientCollation{key: &sdb[i], value: NextVal}
			i += 1
		case strings.EqualFold(sdb[i], utils.TimeZone):
			if i+1 > sdbLength {
				return errors.Errorf("%s Value must be specified", utils.TimeZone)
			}
			NextVal := &sdb[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if s.DBInfo.clientCollation != nil {
				return errors.Errorf("parameters cannot be repeated: %s", *NextVal)
			}

			tm, err := time.LoadLocation(*NextVal)
			if err != nil {
				return errors.Errorf("unknown time zone: %v", *NextVal)
			}
			s.DBInfo.timeZone = &TimeZone{key: &sdb[i], value: tm}
			i += 1
		default:
			return errors.Errorf("unknown keyword: %s", sdb[i])
		}

	}

	if s.DBInfo.port == nil {
		s.DBInfo.port = &PortModel{key: &utils.Port, value: &utils.DefaultPort}
	}
	if s.DBInfo.types == nil {
		s.DBInfo.types = &TypeModel{key: &utils.Types, value: &utils.DefaultTypes}
	}
	if s.DBInfo.userId == nil {
		s.DBInfo.userId = &UserIdModel{key: &utils.UserId, value: &utils.DefaultUserId}
	}
	if s.DBInfo.passWord == nil {
		return errors.Errorf("%s %s must be specified", utils.SourceDBType, utils.PassWord)
	}

	if s.DBInfo.serverId == nil {
		return errors.Errorf("%s %s must be specified", utils.SourceDBType, utils.ServerId)
	}

	if s.DBInfo.retryMaxConnNumber == nil {
		s.DBInfo.retryMaxConnNumber = &RetryMaxConnect{key: &utils.Retry, value: &utils.DefaultMaxRetryConnect}
	}

	if s.DBInfo.clientCharacter == nil && s.DBInfo.clientCollation == nil {
		s.DBInfo.clientCharacter = &ClientCharacterSet{key: &utils.Character, value: &utils.DefaultClientCharacter}
		s.DBInfo.clientCollation = &ClientCollation{key: &utils.Collation, value: &utils.DefaultClientCollation}
	} else {
		return errors.Errorf("character set and collation must be configured at the same time")
	}

	if s.DBInfo.timeZone == nil {
		s.DBInfo.timeZone = &TimeZone{key: &utils.TimeZone, value: utils.DefaultTimeZone}
	}

	return nil
}

func (s *SourceDB) add(raw *string) error {
	return nil
}

type sourceDBSet struct {
	sdb *SourceDB
}

func (sd *sourceDBSet) MarshalJson() ([]byte, error) {
	var db SourceDBJson
	db.Type = sd.sdb.paramPrefix
	db.Address = sd.sdb.DBInfo.GetAddress()
	db.Port = sd.sdb.DBInfo.GetPort()
	db.Types = sd.sdb.DBInfo.GetTypes()
	db.UserId = sd.sdb.DBInfo.GetUserId()
	db.PassWord = sd.sdb.DBInfo.GetPassWord()
	db.ServerId = sd.sdb.DBInfo.GetServerID()
	db.RetryMaxConnNumber = sd.sdb.DBInfo.GetRetryConnect()
	db.ClientCharacter = sd.sdb.DBInfo.GetClientCharacter()
	db.ClientCollation = sd.sdb.DBInfo.GetClientCollation()
	dbs, err := json.Marshal(db)
	return dbs, err
}

var sourceDBSetBus sourceDBSet

func (sd *sourceDBSet) Init() {
	sd.sdb = new(SourceDB)
	sd.sdb.DBInfo = new(dbInfo)
}

func (sd *sourceDBSet) Add(raw *string) error {
	return nil
}

func (sd *sourceDBSet) ListParamText() string {
	return sd.sdb.put()
}

func (sd *sourceDBSet) GetParam() interface{} {
	return sd.sdb
}

func (sd *sourceDBSet) Registry() map[string]Parameter {
	sd.Init()
	return map[string]Parameter{utils.SourceDBType: sd.sdb}
}
