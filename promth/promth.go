package promth

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func Push(pushAddr ,metric ,job string,value float64) (err error) {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: fmt.Sprintf("%s", metric)})
	gauge.Set(value)
	err = push.New(pushAddr, fmt.Sprintf("%s",job)).
		Collector(gauge).Push()
	if err != nil {
		return err
	}
	return err
}
