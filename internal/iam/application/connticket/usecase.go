/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connticket

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"strconv"
	"time"

	"xxx/api/iam/v2/json"
	"xxx/internal/shared/device"
	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket"

	"xxx/pkg/database/sql"
)

var ErrIdentityPubInvalid = errors.New("identity public key is invalid")

type TicketIssuer struct {
	db               sql.DB
	deviceMetaReader DeviceMetaReader
	ticketSigner     TicketSigner
	hub              hubendpoint.Descriptor
}

// New validates the Hub descriptor and creates an issuer with its own copy of
// the Hub public key so later caller changes cannot affect issued tickets.
func New(
	db sql.DB,
	reader DeviceMetaReader,
	signer TicketSigner,
	hub hubendpoint.Descriptor,
) (*TicketIssuer, error) {
	if err := hub.Validate(); err != nil {
		return nil, err
	}

	hub.PublicKey = bytes.Clone(hub.PublicKey)
	return &TicketIssuer{
		db:               db,
		deviceMetaReader: reader,
		ticketSigner:     signer,
		hub:              hub,
	}, nil
}

// Issue reads device metadata from the primary database, checks ownership, active
// state, and identity key, then issues a short-lived signed ticket for the configured
// Hub. The caller owns the response Ticket buffer and must clear it and return it
// to slicepool after use.
func (ti *TicketIssuer) Issue(
	ctx context.Context,
	subject string,
	req json.ConnectionTicketReq,
) (*json.ConnectionTicketResp, error) {
	if req.DeviceID == "" || subject == "" {
		return nil, ErrRequestInvalid
	}

	id, err := strconv.ParseUint(req.DeviceID, 10, 64)
	if err != nil || id == 0 {
		return nil, ErrRequestInvalid
	}

	var deviceMeta *DeviceMeta
	err = ti.db.WithDB(ctx, sql.TxSourcePrimary, func(dbCtx context.Context) error {
		var e error
		deviceMeta, e = ti.deviceMetaReader.FindDeviceMeta(dbCtx, device.ID(id))
		return e
	})
	if err != nil {
		return nil, err
	}
	if deviceMeta == nil || deviceMeta.ExternalID != subject {
		return nil, ErrDeviceNotFound
	}
	if deviceMeta.State != device.StateActive {
		return nil, ErrDeviceUnactive
	}

	err = ticket.ValidateIdentity(deviceMeta.IdentityPub)
	if err != nil {
		return nil, ErrIdentityPubInvalid
	}

	now := time.Now()
	info := &ticket.Info{
		DeviceID:    device.ID(id),
		IssuedAt:    now.Unix(),
		ExpiresAt:   now.Add(ticket.MaxTTL).Unix(),
		Audience:    ti.hub.Audience,
		HubKeyID:    ti.hub.KeyID,
		WireVersion: ti.hub.Version,
	}
	if _, err = rand.Read(info.TicketID[:]); err != nil {
		return nil, err
	}

	copy(info.IdentityPub[:], deviceMeta.IdentityPub)
	copy(info.HubPublicKey[:], ti.hub.PublicKey)

	wire, err := ti.ticketSigner.Sign(info)
	if err != nil {
		return nil, err
	}

	return &json.ConnectionTicketResp{
		Ticket: wire,
		Hub: hubendpoint.Descriptor{
			Address:   ti.hub.Address,
			Audience:  ti.hub.Audience,
			KeyID:     ti.hub.KeyID,
			PublicKey: bytes.Clone(ti.hub.PublicKey),
			Protocol:  ti.hub.Protocol,
			Version:   ti.hub.Version,
		},
	}, nil
}
