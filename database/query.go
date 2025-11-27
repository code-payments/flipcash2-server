package database

import (
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

type QueryOption func(*QueryOptions)

func WithLimit(limit int) QueryOption {
	return func(o *QueryOptions) {
		if limit > 0 {
			o.Limit = limit
		}
	}
}

func WithPagingToken(pagingToken *commonpb.PagingToken) QueryOption {
	return func(o *QueryOptions) {
		o.PagingToken = pagingToken
	}
}

func WithOrder(order commonpb.QueryOptions_Order) QueryOption {
	return func(o *QueryOptions) {
		o.Order = order
	}
}

func WithAscending() QueryOption {
	return func(o *QueryOptions) {
		o.Order = commonpb.QueryOptions_ASC
	}
}

func WithDescending() QueryOption {
	return func(o *QueryOptions) {
		o.Order = commonpb.QueryOptions_DESC
	}
}

type QueryOptions struct {
	Limit       int
	PagingToken *commonpb.PagingToken
	Order       commonpb.QueryOptions_Order
}

func DefaultQueryOptions() QueryOptions {
	return QueryOptions{
		Limit: 100,
		Order: commonpb.QueryOptions_ASC,
	}
}

func ApplyQueryOptions(options ...QueryOption) QueryOptions {
	applied := DefaultQueryOptions()
	for _, option := range options {
		option(&applied)
	}
	return applied
}

func FromProtoQueryOptions(protoOptions *commonpb.QueryOptions) []QueryOption {
	if protoOptions == nil {
		return nil
	}

	options := []QueryOption{WithOrder(protoOptions.Order)}

	if protoOptions.PageSize > 0 {
		options = append(options, WithLimit(int(protoOptions.PageSize)))
	}

	if protoOptions.PagingToken != nil {
		options = append(options, WithPagingToken(protoOptions.PagingToken))
	}

	return options
}
