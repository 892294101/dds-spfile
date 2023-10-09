package dds_spfile

// 序列化进程名称
type ProcessJson struct {
	Type *string `json:"PARAMS_TYPE"`
	Name *string `json:"NAME"`
}

// 序列化进程名称
type TableJson struct {
	Type  *string `json:"PARAMS_TYPE"`
	Owner string  `json:"OWNER"`
	Table string  `json:"TABLE"`
}

type TableExcludeJson struct {
	Type  *string `json:"PARAMS_TYPE"`
	Owner string  `json:"OWNER"`
	Table string  `json:"TABLE"`
}

type TrailDirJson struct {
	Type     *string `json:"PARAMS_TYPE"`
	Dir      *string `json:"DIR"`
	Size     *int    `json:"SIZE"`
	SizeUnit *string `json:"SIZEUNIT"`
	Keep     *int    `json:"KEEP"`
	KeepUnit *string `json:"KEEPUNIT"`
}

type DiscardJson struct {
	Type *string `json:"PARAMS_TYPE"`
	Dir  *string `json:"DIR"`
}

type UserIDJson struct {
	Type               *string  `json:"PARAMS_TYPE"`
	Address            []string `json:"ADDRESS"`            // 数据库地址
	Port               *uint16  `json:"PORT"`               // 数据库端口
	UserName           *string  `json:"USERNAME"`           // 用户名
	Sid                *string  `json:"SID"`                // instance id
	PassWord           *string  `json:"PASSWORD"`           // 密码
	RetryMaxConnNumber *int     `json:"RETRYMAXCONNNUMBER"` // 连接重连最大次数
	ClientCharacter    *string  `json:"CLIENTCHARACTER"`    // 客户端字符集
	TimeZone           *string  `json:"TIMEZONE"`           // 时区
}

type SourceDBJson struct {
	Type               *string `json:"PARAMS_TYPE"`
	Address            *string `json:"ADDRESS"`            // 数据库地址
	Port               *uint16 `json:"PORT"`               // 数据库端口
	Types              *string `json:"DATABASETYPE"`       // 连接数据库类型, mysql或 mariadb
	UserId             *string `json:"USERID"`             // 用户名
	PassWord           *string `json:"PASSWORD"`           // 密码
	ServerId           *uint32 `json:"SERVERID"`           // mysql server id
	RetryMaxConnNumber *int    `json:"RETRYMAXCONNNUMBER"` // 连接重连最大次数
	ClientCharacter    *string `json:"CLIENTCHARACTER"`    // 客户端字符集
	ClientCollation    *string `json:"CLIENTCOLLATION"`    // 客户端字符集
}

type OptsList struct {
	Key   string `json:"KEY"`
	Value bool   `json:"VALUE"`
}

type DBOptionsJson struct {
	Type *string     `json:"PARAMS_TYPE"`
	Opts []*OptsList `json:"OPTIONS"`
}

// 序列化DDL
type DdlJson struct {
	Type    *string  `json:"PARAMS_TYPE"`
	Range   *string  `json:"TYPE"`
	Owner   *string  `json:"OWNER"`
	Table   *string  `json:"TABLE"`
	OpType  []string `json:"OPTYPE"`
	ObjType []string `json:"OBJTYPE"`
}
