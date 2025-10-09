package cluster

// User holds user requirements
type User struct {
	Firstname string `form:"firstname" json:"firstname" binding:"required"`
	Lastname  string `form:"lastname" json:"lastname" binding:"required"`
}

// KV holds user requirements
type KV struct {
	Key   string `form:"key" json:"key" binding:"required"`
	Value string `form:"value" json:"value" binding:"required"`
}
