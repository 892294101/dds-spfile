package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds-utils"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

type ProcessInfo struct {
	Name *string
}

func (p *ProcessInfo) GetName() *string {
	n := strings.ToUpper(*p.Name)
	return &n
}

type Process struct {
	supportParams map[string]map[string]string
	paramPrefix   *string
	ProInfo       *ProcessInfo
}

func (p *Process) put() string {
	return fmt.Sprintf("%s %s\n", *p.paramPrefix, *p.ProInfo.Name)
}

// 初始化参数可以支持的数据库和进程
func (p *Process) init() {
	p.supportParams = map[string]map[string]string{
		ddsutils.MySQL: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
		ddsutils.Oracle: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
	}
}

func (p *Process) initDefault() error {
	return nil
}

func (p *Process) isType(raw *string, dbType *string, processType *string) error {
	p.init()
	_, ok := p.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (p *Process) parse(raw *string) error {
	matched, _ := regexp.MatchString(ddsutils.ProcessRegular, *raw)
	if matched == true {
		rd := strings.Split(*raw, " ")
		p.paramPrefix = &rd[0]
		name := strings.ToLower(rd[1])
		p.ProInfo.Name = &name
		return nil
	}

	return errors.Errorf("%s parameter parsing failed: %s", ddsutils.ProcessType, *raw)
}

func (p *Process) add(raw *string) error {
	return nil
}

type processSet struct {
	process *Process
}

func (p *processSet) MarshalJson() ([]byte, error) {
	var pj ProcessJson
	pj.Type = p.process.paramPrefix
	pj.Name = p.process.ProInfo.Name
	pro, err := json.Marshal(pj)
	return pro, err
}

var ProcessBus processSet

func (p *processSet) Init() {
	p.process = new(Process)
	p.process.ProInfo = new(ProcessInfo)
}

func (p *processSet) Add(raw *string) error {
	return nil
}

func (p *processSet) ListParamText() string {
	return p.process.put()
}

func (p *processSet) GetParam() interface{} {
	return p.process
}

func (p *processSet) Registry() map[string]Parameter {
	p.Init()
	return map[string]Parameter{ddsutils.ProcessType: p.process}
}
