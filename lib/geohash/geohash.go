package geohash

import "encoding/base32"

var bits = []uint8{1 << 7, 1 << 6, 1 << 5, 1 << 4, 1 << 3, 1 << 2, 1 << 1, 1 << 0}
var enc = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)

const (
	defaultBitSize = 64 // 32bit
)

func encode0(latitude, longtitude float64, bitSize uint) ([]byte, [2][2]float64) {
	box := [2][2]float64{
		{-180, -180},
		{-90, -90},
	}

	pos := [2]float64{longtitude, latitude}
	hashLen := bitSize >> 3
	if bitSize&7 > 0 {
		hashLen++
	}
	hash := make([]byte, hashLen)
	var precision uint = 0
	for precision < bitSize {
		for dire, val := range pos {
			mid := (box[dire][0] + box[dire][1]) / 2
			if val < mid {
				box[dire][1] = mid
			} else {
				box[dire][0] = mid

			}
		}
	}
}
