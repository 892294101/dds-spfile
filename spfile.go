package ddsspfile

import (
	"bufio"
	ddsutils "github.com/892294101/dds-utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
)

const (
	AnnotationPrefix = "--"
)

type Spfile struct {
	rawData       []string              // 文件原始数据
	paramBaseInfo *spfileBaseInfo       // 文件句柄
	log           *logrus.Logger        //日志系统
	paramSet      map[string]Parameters // 参数集
	paramSetIndex []string              // 参数集的索引, 因为map不排序
	mustParams    []string              // 必须存在的参数
	confserver    *configServer
}

// 初始化数据库和进程必须存在的参数
func (s *Spfile) init() error {

	s.paramSet = make(map[string]Parameters)
	switch {
	// MySQL extract进程必须存在的参数
	case s.paramBaseInfo.dbType == GetMySQLName() && s.paramBaseInfo.processType == GetExtractName():
		s.mustParams = append(s.mustParams, ddsutils.ProcessType)
		s.mustParams = append(s.mustParams, ddsutils.SourceDBType)
		s.mustParams = append(s.mustParams, ddsutils.TrailDirType)
		s.mustParams = append(s.mustParams, ddsutils.DiscardFileType)
		s.mustParams = append(s.mustParams, ddsutils.DBOptionsType)
		s.mustParams = append(s.mustParams, ddsutils.TableType)
	// Oracle extract进程必须存在的参数
	case s.paramBaseInfo.dbType == GetOracleName() && s.paramBaseInfo.processType == GetExtractName():
		s.mustParams = append(s.mustParams, ddsutils.ProcessType)
		s.mustParams = append(s.mustParams, ddsutils.UserId)
		s.mustParams = append(s.mustParams, ddsutils.TrailDirType)
		s.mustParams = append(s.mustParams, ddsutils.DiscardFileType)
		s.mustParams = append(s.mustParams, ddsutils.DBOptionsType)
		s.mustParams = append(s.mustParams, ddsutils.TableType)
	}

	return nil
}

// 初始化配置服务器
func (s *Spfile) initConfigServer() error {
	s.confserver = new(configServer)
	s.confserver.init(filepath.Join(s.paramBaseInfo.homeDir, "config", "cfgserver.lib"), s.log)
	return s.confserver.open()
}

// 生产参数，必须调用
func (s *Spfile) Production() error {
	if err := s.init(); err != nil {
		return err
	}
	// 初始化配置服务器
	if err := s.initConfigServer(); err != nil {
		return err
	}

	f, err := os.Open(s.paramBaseInfo.file)
	if err != nil {
		return errors.Errorf("Failed to open parameter file %s: %s", s.paramBaseInfo.file, err)
	}
	reader := bufio.NewScanner(f)
	for reader.Scan() {
		val := strings.TrimSpace(reader.Text())
		if !strings.HasPrefix(val, AnnotationPrefix) && val != "" {
			s.rawData = append(s.rawData, val)
		}
	}
	return s.scanParams()
}

// 扫描参数
func (s *Spfile) scanParams() error {
	for _, params := range s.rawData {
		var pro Parameters
		switch {
		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.ProcessType):
			if s.paramSet[ddsutils.ProcessType] == nil {
				pro = &ProcessBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				return errors.Errorf("%s configuration cannot be set repeatedly", ddsutils.ProcessType)
			}
		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.SourceDBType):
			if s.paramSet[ddsutils.SourceDBType] == nil {
				pro = &sourceDBSetBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				return errors.Errorf("%s configuration cannot be set repeatedly", ddsutils.SourceDBType)
			}

		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.TrailDirType):
			if s.paramSet[ddsutils.TrailDirType] == nil {
				pro = &trailDirBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				return errors.Errorf("%s configuration cannot be set repeatedly", ddsutils.TrailDirType)
			}

		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.DiscardFileType):
			if s.paramSet[ddsutils.DiscardFileType] == nil {
				pro = &DiscardFileBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				return errors.Errorf("%s configuration cannot be set repeatedly", ddsutils.DiscardFileType)
			}

		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.DBOptionsType):
			if s.paramSet[ddsutils.DBOptionsType] == nil {
				pro = &DBOptionsBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				return errors.Errorf("%s configuration cannot be set repeatedly", ddsutils.DBOptionsType)
			}

		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.TableType+" "):
			if s.paramSet[ddsutils.TableType] == nil {
				pro = &TableSetBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				pro = s.paramSet[ddsutils.TableType]
				if err := s.addParams(pro, &params); err != nil {
					return err
				}
			}
		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.TableExcludeType):
			if s.paramSet[ddsutils.TableExcludeType] == nil {
				pro = &ExcludeTableSetBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				pro = s.paramSet[ddsutils.TableExcludeType]
				if err := s.addParams(pro, &params); err != nil {
					return err
				}
			}
		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.OUserIDType):
			if s.paramSet[ddsutils.OUserIDType] == nil {
				pro = &userIDSetBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				return errors.Errorf("%s configuration cannot be set repeatedly", ddsutils.ProcessType)
			}
		case ddsutils.HasPrefixIgnoreCase(params, ddsutils.DDL):
			if s.paramSet[ddsutils.DDL] == nil {
				pro = &DdlSmtSetBus
				if err := s.firstParams(pro, &params); err != nil {
					return err
				}
			} else {
				pro = s.paramSet[ddsutils.DDL]
				if err := s.addParams(pro, &params); err != nil {
					return err
				}
			}
		default:
			return errors.Errorf("Unknown parameter: %s", params)
		}

	}
	return s.registerMustParams()
}

func (s *Spfile) LoadToDatabase() error {
	var data []string
	for _, index := range s.paramSetIndex {
		res, err := s.paramSet[index].MarshalJson()
		if err != nil {
			return err
		}
		data = append(data, string(res))
	}
	// 加载参数到db文件
	return s.confserver.LoadJsonToServerConfig(data, s.GetProcessName())
}

// 第一次出现的参数解析
func (s *Spfile) firstParams(pro Parameters, params *string) error {
	for Type, rawData := range pro.Registry() {
		if err := rawData.isType(params, &s.paramBaseInfo.dbType, &s.paramBaseInfo.processType); err != nil {
			return err
		}
		if err := rawData.parse(params); err != nil {
			return err
		}
		s.paramSet[Type] = pro
		s.paramSetIndex = append(s.paramSetIndex, Type)

	}
	return nil
}

// 第二次参数出现，调用添加方式
func (s *Spfile) addParams(pro Parameters, params *string) error {
	return pro.Add(params)
}

// 注册默认参数
func (s *Spfile) registerMustParams() error {
	for _, paramType := range s.mustParams {
		switch paramType {
		case ddsutils.DBOptionsType: // 对缺失的参数补充默认值
			_, ok := s.paramSet[ddsutils.DBOptionsType]
			if !ok {
				s.paramSet[ddsutils.DBOptionsType] = &DBOptionsBus
				for _, parameter := range s.paramSet[ddsutils.DBOptionsType].Registry() {
					if err := parameter.initDefault(); err != nil {
						return err
					}
				}
				s.paramSetIndex = append(s.paramSetIndex, paramType)
			}
		default:
			_, ok := s.paramSet[paramType]
			if !ok {
				return errors.Errorf("The %s parameter must be configured", paramType)
			}
		}

	}
	return nil
}

// 输出所有参数
func (s *Spfile) PutParamsText() {
	var tempStr strings.Builder
	for _, index := range s.paramSetIndex {
		res := s.paramSet[index].ListParamText()
		tempStr.WriteString(res)
	}
	s.log.Infof("\n%s", tempStr.String())
}

// DDL过滤
func (s *Spfile) DDLFilter(owner, table, optype, objtype string) bool {
	return s.paramSet[ddsutils.DDL].GetParam().(*DdlSmt).DdlBus.Filter(owner, table, optype, objtype)
}

func (s *Spfile) DMLFilter(owner, table *string) (bool, error) {
	// 判断 include 是否需要此表
	ok, err := s.DMLInlucdeFilter(owner, table)
	if err != nil {
		return false, err
	}
	// 如果 include 需要表，那么则判断 exclude
	if ok {
		// 判断 exclude 参数
		ok, err = s.DMLExcludeFilter(owner, table)
		if err != nil {
			return false, err
		}
		if ok {
			// 如果 exclude 也找到了此表，则说明表需要排除掉
			return false, nil
		} else {
			// 如果 exclude 没有找到，则说明表是我们需要的
			return true, nil
		}
	} else {
		// 如果 include 不需要此表，那么则不要该表数据，那么也不会进入 exclude 判断
		return false, nil
	}
}

// DML MAP INCLUDE 过滤
func (s *Spfile) DMLInlucdeFilter(owner, table *string) (bool, error) {
	return s.paramSet[ddsutils.TableType].GetParam().(*TableSets).TableBus.Filter(owner, table, s.log)
}

// DML MAP Exclude 过滤
func (s *Spfile) DMLExcludeFilter(owner, table *string) (bool, error) {
	if s.paramSet[ddsutils.TableExcludeType] != nil {
		return s.paramSet[ddsutils.TableExcludeType].GetParam().(*ExcludeTableSets).TableBus.Filter(owner, table, s.log)
	} else {
		return false, nil
	}
}

// 获取Oracle连接串
func (s *Spfile) GetOracleDBConnStr() *OdbInfo {
	return s.paramSet[ddsutils.OUserIDType].GetParam().(*UserId).DBInfo
}

// 获取Mysql连接串
func (s *Spfile) GetMySQLDBConnStr() *dbInfo {
	return s.paramSet[ddsutils.SourceDBType].GetParam().(*SourceDB).DBInfo.GetConnInfo()
}

// 获取trail info
func (s *Spfile) GetTrail() *TrailAttribute {
	return s.paramSet[ddsutils.TrailDirType].GetParam().(*TrailDir).DirTrailAttribute.GetTrail()
}

// 获取进程名称
func (s *Spfile) GetProcessName() *string {
	return s.paramSet[ddsutils.ProcessType].GetParam().(*Process).ProInfo.GetName()
}
