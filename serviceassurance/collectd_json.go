package serviceassurance

import (
        "encoding/json"
	"github.com/prometheus/prometheus/pkg/labels"
        "strconv"
)

// Collectd  ...
type Collectd struct {
        Values         []float64
        Dstypes        []string
        Dsnames        []string
        Time           float64 `json:"time"`
        Interval       float64 `json:"interval"`
        Host           string  `json:"host"`
        Plugin         string  `json:"plugin"`
        PluginInstance string  `json:"plugin_instance"`
        Type           string  `json:"type"`
        TypeInstance   string  `json:"type_instance"`
}

func (c *Collectd) primLabels () labels.Labels {
	lbls := make([]labels.Label, 0, 20)

	lbls = append(lbls, labels.Label{
		Name: "Interval",
		Value: strconv.FormatFloat(c.Interval, 'f', 4, 64),
	})
	lbls = append(lbls, labels.Label{
		Name: "Host",
		Value: c.Host,
	})
	lbls = append(lbls, labels.Label{
		Name: "Plugin",
		Value: c.Plugin,
	})
	lbls = append(lbls, labels.Label{
		Name: "PluginInstance",
		Value: c.PluginInstance,
	})
	lbls = append(lbls, labels.Label{
		Name: "Type",
		Value: c.Type,
	})
	lbls = append(lbls, labels.Label{
		Name: "TypeInstance",
		Value: c.TypeInstance,
	})
	return lbls
}

func (c *Collectd) genPromName(dsnames string) string {
	buf := make([]byte, 0, 256)
	formatstr :=	[]string{"collectd", "_", c.Host, "_", c.Plugin, "_", c.PluginInstance, "_",
			c.Type, "_", c.TypeInstance, "_", dsnames, }
	for _, s := range formatstr {
		buf = append(buf, s...)
	}
	return string(buf)
}

func (c *Collectd) GetTime() int64 {
	return int64(c.Time * 1000)
}

func (c *Collectd) Labels () []labels.Labels {
	plbls := c.primLabels()
	lbls := make([]labels.Labels, len(c.Dstypes))

	for i := 0; i < len(c.Dstypes); i++ {
		lbls[i] = make([]labels.Label, 0, 2 + len(plbls))

		lbls[i] = append(lbls[i], labels.Label{
			Name: "__name__",
			Value: c.genPromName(c.Dsnames[i])})
		lbls[i] = append(lbls[i], labels.Label{
			Name: "Dstypes",
			Value: c.Dstypes[i]})
		lbls[i] = append(lbls[i], labels.Label{
			Name: "Dsnames",
			Value: c.Dsnames[i]})
		lbls[i] = append(lbls[i], plbls...)
	}
	return lbls
}

// Collectd generator from json
func ParseCollectdJson (str string) (*[]Collectd, error) {
	var collectd []Collectd

	err := json.Unmarshal([]byte(str), &collectd)
	if err != nil {
		return nil, err
	}

	return &collectd, nil
}
