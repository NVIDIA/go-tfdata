package cmn

func Assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
