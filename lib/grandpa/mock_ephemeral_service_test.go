// Code generated by MockGen. DO NOT EDIT.
// Source: finalisation.go

// Package grandpa is a generated GoMock package.
package grandpa

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockephemeralService is a mock of ephemeralService interface.
type MockephemeralService struct {
	ctrl     *gomock.Controller
	recorder *MockephemeralServiceMockRecorder
}

// MockephemeralServiceMockRecorder is the mock recorder for MockephemeralService.
type MockephemeralServiceMockRecorder struct {
	mock *MockephemeralService
}

// NewMockephemeralService creates a new mock instance.
func NewMockephemeralService(ctrl *gomock.Controller) *MockephemeralService {
	mock := &MockephemeralService{ctrl: ctrl}
	mock.recorder = &MockephemeralServiceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockephemeralService) EXPECT() *MockephemeralServiceMockRecorder {
	return m.recorder
}

// Run mocks base method.
func (m *MockephemeralService) Run() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run")
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockephemeralServiceMockRecorder) Run() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockephemeralService)(nil).Run))
}

// Stop mocks base method.
func (m *MockephemeralService) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockephemeralServiceMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockephemeralService)(nil).Stop))
}
