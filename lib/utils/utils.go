package utils

// ToCmdLine 将字符串转换为 [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	n := len(cmd)
	ans := make([][]byte, n)
	for i := range n {
		ans[i] = []byte(cmd[i])
	}
	return ans
}

// ToCmdLine2 将命令名和字符串类型的参数转换为 [][]byte
func ToCmdLine2(commandName string, args ...string) [][]byte {
	res := make([][]byte, len(args)+1)
	res[0] = []byte(commandName)
	for i := range args {
		res[i+1] = []byte(args[i])
	}
	return res
}

// ToCmdLine3 将命令名和 []byte 类型的参数转换为 CmdLine
func ToCmdLine3(commandName string, args ...[]byte) [][]byte {
	res := make([][]byte, 1, len(args)+1)
	res[0] = []byte(commandName)
	res = append(res, args...)
	return res
}

// Equals 检查给定的值是否相等
func Equals(a, b any) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// BytesEquals 检查给定的字节切片是否相等
func BytesEquals(a, b []byte) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ConvertRange 将 redis 索引转换为 go 切片索引
// -1 => size-1
// 闭区间 [0, 10] => 左闭右开区间 [0, 9)
// 越界处理 [size, size+1] => [-1, -1]
func ConvertRange(start, end, size int64) (int, int) {
	if start < -size {
		return -1, -1
	}
	if start < 0 {
		start = size + start
	} else if start >= size {
		return -1, -1
	}
	if end < -size {
		return -1, -1
	}
	if end < 0 {
		end = size + end + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}
	if start > end {
		return -1, -1
	}
	return int(start), int(end)
}
