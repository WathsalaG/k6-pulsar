package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.k6.io/k6/js/modules"
)

type RootModule struct{}
type PulsarClient struct {
	client pulsar.Client
}
type Instance struct {
	vu     modules.VU
	client pulsar.Client
}

func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &Instance{vu: vu}
}

func init() {
	modules.Register("k6/x/pulsar", &RootModule{})
}

func (i *Instance) Connect(url string) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})
	if err != nil {
		return err
	}
	i.client = client
	return nil
}

func (i *Instance) Send(topic string, message []byte) error {
	producer, err := i.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: message,
	})
	return err
}