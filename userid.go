package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds/utils"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
)

type UserIdIpSet struct {
	key   *string
	value []string
}

type UserIdPortModel struct {
	key   *string
	value *uint16
}

type UserIdUserIdModel struct {
	key   *string
	value *string
}

type UserIdSidModel struct {
	key   *string
	value *string
}

type UserIdPassWordModel struct {
	key   *string
	value *string
}

type UserIdServerIdModel struct {
	key   *string
	value *uint32
}

type UserIdRetryMaxConnect struct {
	key   *string
	value *int
}

type UserIdClientCharacterSet struct {
	key   *string
	value *string
}

type UserIdTimeZone struct {
	key   *string
	value *string
}

func (u *UserIdIpSet) SetIpAddress(ip *string) error {
	ipSet := strings.Split(*ip, ",")
	u.key = &utils.OUserIDType
	for _, v := range ipSet {
		match, _ := regexp.MatchString(IpV4Reg, v)
		if !match {
			return errors.Errorf("%s is an illegal IPV4 address\n", v)
		}
		u.value = append(u.value, v)
	}
	return nil
}

func (u *UserIdUserIdModel) SetUserId(uid *string) {
	u.key = &utils.OUser
	u.value = uid
}

type OdbInfo struct {
	address            *UserIdIpSet              // 数据库地址
	port               *UserIdPortModel          // 数据库端口
	userName           *UserIdUserIdModel        // 用户名
	passWord           *UserIdPassWordModel      // 密码
	sid                *UserIdSidModel           // instance id
	retryMaxConnNumber *UserIdRetryMaxConnect    // 连接重连最大次数
	clientCharacter    *UserIdClientCharacterSet // 客户端字符集
	timeZone           *UserIdTimeZone           // 时区
}

func (u *OdbInfo) setUserId(uid *string) error {
	uidSet := strings.Split(*uid, "@")
	if len(uidSet) == 2 {
		u.userName = &UserIdUserIdModel{}
		u.address = &UserIdIpSet{}
		u.userName.SetUserId(&uidSet[0])
		if err := u.address.SetIpAddress(&uidSet[1]); err != nil {
			return err
		}
		return nil
	}
	return errors.Errorf("Please specify user Name and database host address <username>@<HostAddress,[...HostAddress]>")

}

func (u *OdbInfo) GetConnInfo() *OdbInfo {
	return u
}

func (u *OdbInfo) GetHostAddress() []string {
	return u.address.value
}

func (u *OdbInfo) GetPort() uint16 {
	return *u.port.value
}

func (u *OdbInfo) GetUserName() string {
	return *u.userName.value
}

func (u *OdbInfo) GetPassWord() string {
	return *u.passWord.value
}

func (u *OdbInfo) GetSID() string {
	return *u.sid.value
}

func (u *OdbInfo) GetRetryMaxConnNumber() int {
	return *u.retryMaxConnNumber.value
}

func (u *OdbInfo) GetClientCharacter() string {
	return *u.clientCharacter.value
}

func (u *OdbInfo) GetTimeZone() string {
	return *u.timeZone.value
}

type UserId struct {
	supportParams map[string]map[string]string `json:"_"`           // 参数支持吃数据库和进程
	ParamPrefix   *string                      `json:"PARAMS_TYPE"` // 参数前缀
	DBInfo        *OdbInfo                     `json:"DBINFO"`
}

// 初始化参数可以支持的数据库和进程

func (u *UserId) init() {
	u.supportParams = map[string]map[string]string{
		utils.Oracle: {
			utils.Extract:  utils.Extract,
			utils.Replicat: utils.Replicat,
		},
	}
}

func (u *UserId) initDefault() error {
	return nil
}

func (u *UserId) put() string {

	return fmt.Sprintf("%s %s@%s %s %d %s %s %s %s %s %s\n", *u.ParamPrefix,
		*u.DBInfo.userName.value,
		*utils.SliceToString(u.DBInfo.address.value, ","),
		*u.DBInfo.port.key,
		*u.DBInfo.port.value,
		*u.DBInfo.passWord.key,
		*u.DBInfo.passWord.value,
		*u.DBInfo.clientCharacter.key,
		*u.DBInfo.clientCharacter.value,
		*u.DBInfo.timeZone.key,
		*u.DBInfo.timeZone.value)
}

func (u *UserId) isType(raw *string, dbType *string, processType *string) error {
	u.init()
	_, ok := u.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (u *UserId) parse(raw *string) error {
	uid := utils.TrimKeySpace(strings.Split(*raw, " "))
	uidLength := len(uid) - 1

	for i := 0; i < len(uid); i++ {
		switch {
		case strings.EqualFold(uid[i], utils.OUserIDType):
			u.ParamPrefix = &uid[i]
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.OUserIDType)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.address != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			if err := u.DBInfo.setUserId(NextVal); err != nil {
				return err
			}
			i += 1
		case strings.EqualFold(uid[i], utils.OPassWord):
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.OPassWord)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.passWord != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			u.DBInfo.passWord = &UserIdPassWordModel{&uid[i], NextVal}
			i += 1
		case strings.EqualFold(uid[i], utils.OSid):
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.OSid)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.sid != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			u.DBInfo.sid = &UserIdSidModel{&uid[i], NextVal}
			i += 1
		case strings.EqualFold(uid[i], utils.OPort):
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.OPort)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.port != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			match, _ := regexp.MatchString(IpV4Port, *NextVal)
			if !match {
				return errors.Errorf("%s is an illegal IPV4 Port\n", *NextVal)
			}

			p, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s %s conversion failed", utils.OPort, *NextVal)
			}
			port := uint16(p)

			u.DBInfo.port = &UserIdPortModel{&uid[i], &port}
			i += 1
		case strings.EqualFold(uid[i], utils.ORetry):
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.ORetry)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.retryMaxConnNumber != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			p, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s %s conversion failed", utils.ORetry, *NextVal)
			}
			u.DBInfo.retryMaxConnNumber = &UserIdRetryMaxConnect{&uid[i], &p}
			i += 1
		case strings.EqualFold(uid[i], utils.OCharacter):
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.OCharacter)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.clientCharacter != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			u.DBInfo.clientCharacter = &UserIdClientCharacterSet{&uid[i], NextVal}
			i += 1
		case strings.EqualFold(uid[i], utils.OTimeZone):
			if i+1 > uidLength {
				return errors.Errorf("%s Value must be specified", utils.OTimeZone)
			}
			NextVal := &uid[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if u.DBInfo.timeZone != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			u.DBInfo.timeZone = &UserIdTimeZone{&uid[i], NextVal}
			i += 1
		default:
			return errors.Errorf("unknown keyword: %s", uid[i])
		}

	}

	if u.DBInfo.port == nil {
		u.DBInfo.port = &UserIdPortModel{&utils.OPort, &utils.ODefaultPort}
	}

	if u.DBInfo.sid == nil {
		return errors.Errorf("%s %s must be specified", utils.OUserIDType, utils.OSid)
	}

	if u.DBInfo.passWord == nil {
		return errors.Errorf("%s %s must be specified", utils.OUserIDType, utils.OPassWord)
	}

	if u.DBInfo.retryMaxConnNumber == nil {
		u.DBInfo.retryMaxConnNumber = &UserIdRetryMaxConnect{&utils.ORetry, &utils.ODefaultMaxRetryConnect}
	}

	if u.DBInfo.clientCharacter == nil {
		return errors.Errorf("%s %s must be specified", utils.OUserIDType, utils.OCharacter)
	}

	if u.DBInfo.timeZone == nil {
		u.DBInfo.timeZone = &UserIdTimeZone{&utils.OTimeZone, &utils.ODefaultTimeZone}
	}

	return nil
}

func (u *UserId) add(raw *string) error {
	return nil
}

type userIDSet struct {
	uid *UserId
}

func (u *userIDSet) MarshalJson() ([]byte, error) {
	var ud UserIDJson
	ud.Type = u.uid.ParamPrefix
	ud.Address = u.uid.DBInfo.address.value
	ud.Port = u.uid.DBInfo.port.value
	ud.UserName = u.uid.DBInfo.userName.value
	ud.Sid = u.uid.DBInfo.sid.value
	ud.PassWord = u.uid.DBInfo.passWord.value
	ud.RetryMaxConnNumber = u.uid.DBInfo.retryMaxConnNumber.value
	ud.ClientCharacter = u.uid.DBInfo.clientCharacter.value
	ud.TimeZone = u.uid.DBInfo.timeZone.value
	uj, err := json.Marshal(ud)
	return uj, err
}

var userIDSetBus userIDSet

func (u *userIDSet) Init() {
	u.uid = new(UserId)
	u.uid.DBInfo = new(OdbInfo)
}

func (u *userIDSet) Add(raw *string) error {
	return nil
}

func (u *userIDSet) ListParamText() string {
	return u.uid.put()
}

func (u *userIDSet) GetParam() interface{} {
	return u.uid
}

func (u *userIDSet) Registry() map[string]Parameter {
	u.Init()
	return map[string]Parameter{utils.OUserIDType: u.uid}
}
