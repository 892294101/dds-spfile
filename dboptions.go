package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds-utils"
	"github.com/pkg/errors"
	"strings"
)

type Options struct {
	opts map[string]bool
}

func (o *Options) setOption(s *string) error {
	ops := strings.ToUpper(*s)
	switch ops {
	case ddsutils.GetReplicates:
		o.opts[ddsutils.GetReplicates] = true
	case ddsutils.IgnoreReplicates:
		o.opts[ddsutils.GetReplicates] = false
	default:
		ops := strings.ToUpper(*s)
		_, ok := o.opts[ops]
		if ok {
			o.opts[ops] = true
		} else {
			return errors.Errorf("unknown parameter: %s", *s)
		}
	}
	return nil
}

func (o *Options) GetReplicates() (*bool, error) {
	v, ok := o.opts[ddsutils.GetReplicates]
	if ok {
		return &v, nil
	}
	return nil, errors.Errorf("%s Parameter Value acquisition failed", ddsutils.GetReplicates)
}

func (o *Options) GetSuppressionTrigger() (*bool, error) {
	v, ok := o.opts[ddsutils.SuppressionTrigger]
	if ok {
		return &v, nil
	}
	return nil, errors.Errorf("%s Parameter Value acquisition failed", ddsutils.SuppressionTrigger)
}

func (o *Options) GetIgnoreForeignkey() (*bool, error) {
	v, ok := o.opts[ddsutils.IgnoreForeignkey]
	if ok {
		return &v, nil
	}
	return nil, errors.Errorf("%s Parameter Value acquisition failed", ddsutils.IgnoreForeignkey)
}

type DBOptions struct {
	supportParams map[string]map[string]string
	paramPrefix   *string
	OptionsSet    *Options
}

func (d *DBOptions) put() string {
	var msg string
	msg += fmt.Sprintf("%s", *d.paramPrefix)
	for s, b := range d.OptionsSet.opts {
		msg += fmt.Sprintf(" %s %v", s, b)
	}
	msg += fmt.Sprintf("\n")
	return msg
}

// 当传入参数时, 初始化特定参数的值
func (d *DBOptions) init() {
	d.supportParams = map[string]map[string]string{
		ddsutils.MySQL: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
		ddsutils.Oracle: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
	}
	d.OptionsSet = &Options{
		opts: map[string]bool{
			ddsutils.SuppressionTrigger: false,
			ddsutils.IgnoreForeignkey:   false,
			ddsutils.GetReplicates:      false,
		},
	}
}

// 当没有参数时, 初始化此参数默认值
func (d *DBOptions) initDefault() error {
	d.init()
	d.paramPrefix = &ddsutils.DBOptionsType
	return nil
}

func (d *DBOptions) isType(raw *string, dbType *string, processType *string) error {
	d.init()
	_, ok := d.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (d *DBOptions) parse(raw *string) error {
	options := ddsutils.TrimKeySpace(strings.Split(*raw, " "))
	optionsLength := len(options) - 1
	for i := 0; i < len(options); i++ {
		if strings.EqualFold(options[i], ddsutils.DBOptionsType) {
			d.paramPrefix = &options[i]
			if i+1 > optionsLength {
				return errors.Errorf("%s Value must be specified", options[i])
			}
		} else {
			err := d.OptionsSet.setOption(&options[i])
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (d *DBOptions) add(raw *string) error {
	return nil
}

type DBOptionsSet struct {
	dbOps *DBOptions
}

func (d *DBOptionsSet) MarshalJson() ([]byte, error) {
	var dbo DBOptionsJson
	dbo.Type = d.dbOps.paramPrefix
	for k, v := range d.dbOps.OptionsSet.opts {
		dbo.Opts = append(dbo.Opts, &OptsList{Key: k, Value: v})
	}
	dbos, err := json.Marshal(dbo)
	return dbos, err
}

var DBOptionsBus DBOptionsSet

func (d *DBOptionsSet) Init() {
	d.dbOps = new(DBOptions)
}

func (d *DBOptionsSet) Add(raw *string) error {
	return nil
}

func (d *DBOptionsSet) ListParamText() string {
	return d.dbOps.put()
}

func (d *DBOptionsSet) GetParam() interface{} {
	return d.dbOps
}

func (d *DBOptionsSet) Registry() map[string]Parameter {
	d.Init()
	return map[string]Parameter{ddsutils.DBOptionsType: d.dbOps}
}
