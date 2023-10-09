package dds_spfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds/utils"
	"github.com/pkg/errors"
	"strings"
)

type DiscardFile struct {
	supportParams map[string]map[string]string
	paramPrefix   *string
	Dir           *string
}

func (d *DiscardFile) put() string {
	return fmt.Sprintf("%s %s\n", *d.paramPrefix, *d.Dir)
}

func (d *DiscardFile) init() {
	d.supportParams = map[string]map[string]string{
		utils.MySQL: {
			utils.Extract:  utils.Extract,
			utils.Replicat: utils.Replicat,
		},
		utils.Oracle: {
			utils.Extract:  utils.Extract,
			utils.Replicat: utils.Replicat,
		},
	}
}

func (d *DiscardFile) initDefault() error {
	return nil
}

func (d *DiscardFile) isType(raw *string, dbType *string, processType *string) error {
	d.init()
	_, ok := d.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (d *DiscardFile) parse(raw *string) error {
	discards := utils.TrimKeySpace(strings.Split(*raw, " "))
	discardLength := len(discards) - 1
	for i := 0; i < len(discards); i++ {
		switch {
		case strings.EqualFold(discards[i], utils.DiscardFileType):
			d.paramPrefix = &discards[i]
			if i+1 > discardLength {
				return errors.Errorf("%s Value must be specified", discards[i])
			}
			NextVal := &discards[i+1]
			if utils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if d.Dir != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			d.Dir = NextVal
			i += 1
		default:
			return errors.Errorf("unknown parameter: %s", discards[i])
		}
	}

	return nil
}

func (d *DiscardFile) add(raw *string) error {

	return nil
}

type DiscardFileSet struct {
	discard *DiscardFile
}

func (d *DiscardFileSet) MarshalJson() ([]byte, error) {
	var sj DiscardJson
	sj.Type = d.discard.paramPrefix
	sj.Dir = d.discard.Dir
	sjs, err := json.Marshal(sj)
	return sjs, err
}

var DiscardFileBus DiscardFileSet

func (d *DiscardFileSet) Init() {
	d.discard = new(DiscardFile)
}

func (d *DiscardFileSet) Add(raw *string) error {
	return nil
}

func (d *DiscardFileSet) ListParamText() string {
	return d.discard.put()
}

func (d *DiscardFileSet) GetParam() interface{} {
	return d.discard
}

func (d *DiscardFileSet) Registry() map[string]Parameter {
	d.Init()
	return map[string]Parameter{utils.DiscardFileType: d.discard}
}
