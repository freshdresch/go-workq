package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/examples/internal/inventory"
)

func receiptFixture(t *testing.T) (inventory.Request, string, []string) {
	t.Helper()
	request := inventory.Request{
		Key:   inventory.Key{Namespace: "parser-test", Sequence: 1, Effect: "inventory.adjust"},
		Codec: inventory.Codec, Payload: []byte(`{"item":"widget","delta":3}`),
	}
	key, err := request.Key.Encode()
	require.NoError(t, err)
	fields := []string{
		`"state":"succeeded"`,
		fmt.Sprintf(`"key":%q`, key),
		fmt.Sprintf(`"codec":%q`, request.Codec),
		fmt.Sprintf(`"request":%q`, base64.StdEncoding.EncodeToString(request.Payload)),
		fmt.Sprintf(`"result":{"adjustment_id":%q,"quantity":0}`, key),
	}
	return request, key, fields
}

func receiptBody(fields []string) string {
	return "{" + strings.Join(fields, ",") + "}"
}

func assertInvalidReceipt(t *testing.T, body string) {
	t.Helper()
	receipt, err := decodeReceiptResponse([]byte(body))
	assert.Error(t, err)
	assert.Equal(t, receiptResponse{}, receipt)
}

func TestReceiptResponseTopLevelFields(t *testing.T) {
	t.Parallel()
	_, _, fields := receiptFixture(t)
	for index, name := range []string{"state", "key", "codec", "request", "result"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			prefix := fmt.Sprintf(`%q:`, name)
			alias := strings.ToUpper(name[:1]) + name[1:]
			escaped := fmt.Sprintf(`"\u%04x%s":`, name[0], name[1:])
			cases := []struct {
				name   string
				fields []string
			}{
				{name: "duplicate", fields: append(slices.Clone(fields), fields[index])},
				{name: "escaped duplicate", fields: append(slices.Clone(fields),
					strings.Replace(fields[index], prefix, escaped, 1))},
				{name: "case alias pair", fields: append(slices.Clone(fields),
					strings.Replace(fields[index], prefix, fmt.Sprintf(`%q:`, alias), 1))},
				{name: "missing", fields: slices.Delete(slices.Clone(fields), index, index+1)},
			}
			for _, replacement := range []struct{ name, value string }{
				{name: "null", value: prefix + "null"},
				{name: "wrong type", value: prefix + "true"},
				{name: "case alias", value: strings.Replace(fields[index],
					prefix, fmt.Sprintf(`%q:`, alias), 1)},
			} {
				changed := slices.Clone(fields)
				changed[index] = replacement.value
				cases = append(cases, struct {
					name   string
					fields []string
				}{name: replacement.name, fields: changed})
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					assertInvalidReceipt(t, receiptBody(tc.fields))
				})
			}
		})
	}
}

func TestReceiptResponseResultShape(t *testing.T) {
	t.Parallel()
	_, key, fields := receiptFixture(t)
	id := fmt.Sprintf(`"adjustment_id":%q`, key)
	cases := []struct{ name, result string }{
		{name: "null", result: `null`},
		{name: "array", result: `[]`},
		{name: "string", result: `"result"`},
		{name: "number", result: `1`},
		{name: "empty", result: `{}`},
		{name: "missing ID", result: `{"quantity":0}`},
		{name: "missing quantity", result: `{` + id + `}`},
		{name: "null ID", result: `{"adjustment_id":null,"quantity":0}`},
		{name: "numeric ID", result: `{"adjustment_id":1,"quantity":0}`},
		{name: "null quantity", result: `{` + id + `,"quantity":null}`},
		{name: "fractional quantity", result: `{` + id + `,"quantity":0.5}`},
		{name: "quoted quantity", result: `{` + id + `,"quantity":"0"}`},
		{name: "overflow", result: `{` + id + `,"quantity":9223372036854775808}`},
		{name: "duplicate ID", result: `{` + id + `,` + id + `,"quantity":0}`},
		{name: "conflicting ID", result: `{` + id + `,"adjustment_id":"other","quantity":0}`},
		{name: "duplicate quantity", result: `{` + id + `,"quantity":0,"quantity":0}`},
		{name: "conflicting quantity", result: `{` + id + `,"quantity":0,"quantity":1}`},
		{name: "quantity alias", result: `{` + id + `,"Quantity":0}`},
		{name: "quantity alias pair", result: `{` + id + `,"quantity":0,"Quantity":0}`},
		{name: "ID alias", result: `{"Adjustment_id":"id","quantity":0}`},
		{name: "ID alias pair", result: `{` + id + `,"Adjustment_id":"id","quantity":0}`},
		{name: "escaped duplicate", result: `{` + id + `,"quantity":0,"qu\u0061ntity":0}`},
		{name: "unknown field", result: `{` + id + `,"quantity":0,"other":{}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := slices.Clone(fields)
			changed[4] = `"result":` + tc.result
			assertInvalidReceipt(t, receiptBody(changed))
		})
	}
}

func TestReceiptResponseRequestEncoding(t *testing.T) {
	t.Parallel()
	_, _, fields := receiptFixture(t)
	for _, value := range []string{
		`[123,125]`, `null`, `1`, `true`, `{}`, `"!"`, `"Zh=="`,
		`"Zg==\n"`, `"Zg"`, `"_w=="`,
	} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			changed := slices.Clone(fields)
			changed[3] = `"request":` + value
			assertInvalidReceipt(t, receiptBody(changed))
		})
	}
}

func TestReceiptResponseOuterShape(t *testing.T) {
	t.Parallel()
	_, _, fields := receiptFixture(t)
	valid := receiptBody(fields)
	for _, body := range []string{
		`null`, `[]`, `"receipt"`, `true`, `1`, ``, `{`, `{}`, valid + `{}`, valid + `x`,
		strings.TrimSuffix(valid, "}") + `,"unknown":{}}`,
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()
			assertInvalidReceipt(t, body)
		})
	}
}

func TestReceiptResponseCompleteAndBounded(t *testing.T) {
	t.Parallel()
	request, key, fields := receiptFixture(t)
	slices.Reverse(fields)
	valid := " \n" + receiptBody(fields)
	body := valid + strings.Repeat(" ", maxResponseBytes-len(valid))
	result, err := matchingResult([]byte(body), key, request)
	require.NoError(t, err)
	assert.Equal(t, inventory.Result{AdjustmentID: key, Quantity: 0}, result)
	result, err = matchingResult([]byte(body+" "), key, request)
	assert.ErrorIs(t, err, inventory.ErrUnresolved)
	assert.Equal(t, inventory.Result{}, result)
	assertInvalidReceipt(t, body+" ")
}

func TestStateResponseRejectsAmbiguity(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"state":"conflict","state":"conflict"}`,
		`{"state":"unresolved","state":"conflict"}`,
		`{"state":"conflict","st\u0061te":"conflict"}`,
		`{"State":"conflict"}`, `{"state":"conflict","State":"conflict"}`,
		`{"state":"conflict","extra":1}`, `{"state":null}`, `{"state":1}`,
		`{"state":[]}`, `{"state":{}}`, `{}`, `null`, `[]`, `"conflict"`,
		`{"state":`, `{"state":"conflict"}{}`, `{"state":"conflict"}x`,
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()
			envelope, err := decodeStateResponse([]byte(body))
			assert.Error(t, err)
			assert.Equal(t, stateEnvelope{}, envelope)
			for _, status := range []int{409, 422, 507} {
				assert.ErrorIs(t, responseError(http.MethodPost, status, []byte(body)),
					inventory.ErrUnresolved)
			}
		})
	}
}

func TestStateResponseValidAndBounded(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		state  string
		status int
		want   error
	}{
		{state: "conflict", status: 409, want: inventory.ErrConflict},
		{state: "rejected", status: 422, want: inventory.ErrRejected},
		{state: "capacity", status: 507, want: inventory.ErrCapacity},
		{state: "malformed", status: 400, want: inventory.ErrRejected},
		{state: "malformed", status: 413, want: inventory.ErrRejected},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			t.Parallel()
			valid := fmt.Sprintf(`{"state":%q}`, tc.state)
			body := valid + strings.Repeat(" ", maxResponseBytes-len(valid))
			envelope, err := decodeStateResponse([]byte(body))
			require.NoError(t, err)
			assert.Equal(t, tc.state, envelope.State)
			assert.ErrorIs(t, responseError(http.MethodPost, tc.status, []byte(body)), tc.want)
			envelope, err = decodeStateResponse([]byte(body + " "))
			assert.Error(t, err)
			assert.Equal(t, stateEnvelope{}, envelope)
			assert.ErrorIs(t, responseError(http.MethodPost, tc.status, []byte(body+" ")),
				inventory.ErrUnresolved)
		})
	}
}
