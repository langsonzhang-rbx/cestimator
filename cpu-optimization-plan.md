# writeRequestUnmarshaler Optimization Plan

Source: `app/cestorage/protoparser/write_request.go`
Profile: `pprof.cestorage.samples.cpu.001.pb` (30s, 279.50s total samples)

## Hotspot Summary

| Function | Flat | Cum | % Total |
|---|---|---|---|
| `TimeSerie.unmarshalProtobuf` | 34.02s | 118.69s | 42.47% |
| `Label.unmarshalProtobuf` | 17.32s | 43.87s | 15.70% |
| `easyproto.NextField` | 32.49s | 36.05s | 12.90% |
| `slices.Contains` + `slices.Index` | — | 11.64s | 4.16% |
| `append(fpBuf, label.Name/Value...)` | — | ~23.5s | ~8.4% |
| `fingerprint` / metro hash | — | 7.14s | 2.55% |

---

## Optimization 1: Incremental hashing — eliminate `fpBuf` accumulation

**Estimated savings: ~25–30s**

### Problem
`TimeSerie.unmarshalProtobuf` (lines 117–154) builds a `fpBuf` by appending name+`\x00`+value+`\x00` for each label, then calls `metro.Hash64(fpBuf, 1337)` at the end. This requires:
- 4 `append` calls per label → repeated `runtime.memmove` as buffer grows
- A second full pass over all label data for hashing

Breakdown of fpBuf-related cost:
- `append(fpBuf, label.Name...)` line 143: 9.53s cum
- `append(fpBuf, label.Value...)` line 145: 13.96s cum
- `fingerprint(fpBuf)` line 154: 7.14s cum
- `runtime.memmove`: 17.68s flat (shared with insertMany but fpBuf appends are a major contributor)

### Solution
Replace `fpBuf []byte` + end-of-series `metro.Hash64` with an incremental `xxhash.Digest` that is fed as each label is parsed. This eliminates all fpBuf appends and the two-pass cost.

### Implementation

**Dependencies:** Add `github.com/cespare/xxhash/v2` to `go.mod` and vendor.

**`writeRequestUnmarshaler` struct:** replace `fpBuf []byte` with `hasher xxhash.Digest`

```go
// before
type writeRequestUnmarshaler struct {
    tss        []TimeSerie
    fpBuf      []byte
    labelsPool []Label
}

// after
import "github.com/cespare/xxhash/v2"

type writeRequestUnmarshaler struct {
    tss        []TimeSerie
    hasher     xxhash.Digest
    labelsPool []Label
}
```

**`Reset()`:** remove `wru.fpBuf = wru.fpBuf[:0]` (hasher is reset per-series, not here)

**`UnmarshalProtobuf`:** pass `&wru.hasher` instead of `fpBuf`; remove fpBuf tracking

```go
// before
labelsPool := wru.labelsPool
fpBuf := wru.fpBuf
// ...
labelsPool, fpBuf, err = ts.unmarshalProtobuf(data, groupLabels, labelsPool, fpBuf)
// ...
wru.labelsPool = labelsPool
wru.fpBuf = fpBuf

// after
labelsPool := wru.labelsPool
// ...
labelsPool, err = ts.unmarshalProtobuf(data, groupLabels, labelsPool, &wru.hasher)
// ...
wru.labelsPool = labelsPool
```

**`TimeSerie.unmarshalProtobuf` signature and body:**

```go
// before
func (ts *TimeSerie) unmarshalProtobuf(src []byte, groupLabels []string, labelsPool []Label, fpBuf []byte) ([]Label, []byte, error) {
    fpBuf = fpBuf[:0]
    // ...per label:
    fpBuf = append(fpBuf, label.Name...)
    fpBuf = append(fpBuf, 0x00)
    fpBuf = append(fpBuf, label.Value...)
    fpBuf = append(fpBuf, 0x00)
    // ...
    ts.Fingerprint = fingerprint(fpBuf)
    return labelsPool, fpBuf, nil
}

// after
func (ts *TimeSerie) unmarshalProtobuf(src []byte, groupLabels []string, labelsPool []Label, h *xxhash.Digest) ([]Label, error) {
    h.Reset()
    // ...per label (using unsafe zero-copy string→[]byte):
    h.Write(s2b(label.Name))
    h.Write(sep)
    h.Write(s2b(label.Value))
    h.Write(sep)
    // ...
    ts.Fingerprint = h.Sum64()
    return labelsPool, nil
}

var sep = []byte{0x00}

// zero-copy string to []byte — safe since bytes are never modified
func s2b(s string) []byte {
    return unsafe.Slice(unsafe.StringData(s), len(s))
}
```

**Remove** `fingerprint` function and `go-metro` import (can be removed from `go.mod` if unused elsewhere).

---

## Optimization 2: Inline `Label.unmarshalProtobuf` into `TimeSerie.unmarshalProtobuf`

**Estimated savings: ~10–15s**

### Problem
For each label, `TimeSerie.unmarshalProtobuf` calls `label.unmarshalProtobuf(data)` which:
- Allocates a fresh `easyproto.FieldContext` on the stack (3.58s flat at line 163)
- Runs a separate `NextField` loop (22.58s cum from Label level)
- Stores strings into heap struct fields triggering `gcWriteBarrier` (2.07s)

### Solution
Inline the label name/value parsing directly in the outer loop. Use `fc.Bytes()` instead of `fc.String()` to get raw byte slices (no string allocation, no GC write barrier).

```go
case 1: // label message
    data, ok := fc.MessageData()
    // parse name and value inline without Label struct for non-group labels
    var nameBytes, valueBytes []byte
    var lfc easyproto.FieldContext
    for len(data) > 0 {
        data, err = lfc.NextField(data)
        switch lfc.FieldNum {
        case 1: nameBytes, _ = lfc.Bytes()
        case 2: valueBytes, _ = lfc.Bytes()
        }
    }
    // feed into hash (zero-copy)
    h.Write(nameBytes); h.Write(sep)
    h.Write(valueBytes); h.Write(sep)
    // only allocate Label struct if it's a group label
    if slices.Contains(groupLabels, string(nameBytes)) {  // replaced by map in opt 3
        labelsPool = append(labelsPool, Label{Name: string(nameBytes), Value: string(valueBytes)})
    }
```

---

## Optimization 3: Replace `slices.Contains` with `map[string]struct{}`

**Estimated savings: ~10s**

### Problem
`slices.Contains(groupLabels, label.Name)` (line 148) is O(n) for every label of every time series. From pprof: 7.84s in `slices.Index` + 1.38s in `memeqbody` = 11.64s total.

### Solution
Convert `groupLabels []string` to `map[string]struct{}` once before the loop (in `UnmarshalProtobuf` or at the call site), pass it down:

```go
groupSet := make(map[string]struct{}, len(groupLabels))
for _, g := range groupLabels {
    groupSet[g] = struct{}{}
}
// ...
if _, ok := groupSet[string(nameBytes)]; ok { ... }
```

---

## Priority and Combined Estimate

| # | Optimization | Estimated Savings |
|---|---|---|
| 1 | Incremental xxhash, eliminate fpBuf | ~25–30s |
| 2 | Inline Label.unmarshalProtobuf | ~10–15s |
| 3 | map[string]struct{} for groupLabels | ~10s |

**Total estimated savings: ~45–55s out of 118.69s** in `TimeSerie.unmarshalProtobuf` (~40–46% reduction).

Note: Optimizations 2 and 3 are synergistic — inlining (opt 2) makes the map lookup (opt 3) on raw bytes trivial and avoids all string allocations for non-group labels.
