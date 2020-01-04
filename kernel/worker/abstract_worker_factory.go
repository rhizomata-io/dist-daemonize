package worker

import "errors"

// AbstractWorkerFactory implements worker.Factory, job data format : #factoryName:data
type AbstractWorkerFactory struct {
	name            string
	workerFactories map[string]Factory
}

// Name return factory.name
func (abstractFactory *AbstractWorkerFactory) Name() string { return abstractFactory.name }

// AddFactory add worker factory
func (abstractFactory *AbstractWorkerFactory) AddFactory(factory Factory) {
	abstractFactory.workerFactories[factory.Name()] = factory
}

// GetFactory get worker factory
func (abstractFactory *AbstractWorkerFactory) GetFactory(name string) (factory Factory, err error) {
	factory = abstractFactory.workerFactories[name]
	if factory == nil {
		err = errors.New("Factory not found for " + name)
	}
	return factory, err
}

// NewAbstractWorkerFactory create AbstractWorkerFactory
func NewAbstractWorkerFactory(name string) (factory *AbstractWorkerFactory) {
	factory = &AbstractWorkerFactory{name: name}
	factory.workerFactories = make(map[string]Factory)
	return factory
}

// NewWorker implements worker.Factory.NewWorker
func (abstractFactory *AbstractWorkerFactory) NewWorker(helper *Helper) (wroker Worker, err error) {
	job := helper.Job()
	pib := job.GetPIandBody()

	if pib == nil {
		err = errors.New("Job Data must be started with '#factory-name:'")
		return nil, err
	}

	factory, err := abstractFactory.GetFactory(pib.PI)
	if err != nil {
		return nil, err
	}

	helper.job.Data = pib.Body
	return factory.NewWorker(helper)

}
