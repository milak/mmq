package item

import (
	"io"
	"math"
	"time"
	"github.com/milak/tools/data"
	"github.com/milak/mmqapi/env"
	"github.com/google/uuid"
)

const PROPERTY_ID = "id"
const PROPERTY_SIZE = "size"
const PROPERTY_PRIORITY = "priority"
const PROPERTY_CREATION_DATE = "creation_date"

/*type Item struct {
	io.Reader
	ID 				string
	Topics	 		[]string
	Reset()
	Properties() 	[]Property
	AddProperty(aName,aValue string) *Property
}*/
type Item struct {
	ID 				string
	Topics			[]string
	CreationDate	time.Time
	value 			[]byte
	ptr 			int
	Properties 		[]data.Property
	shared			bool
}
func NewItem (aTopics []string) *Item{
	result := &Item{ID : uuid.New().String(), CreationDate : time.Now(), Topics : aTopics, ptr : 0}
	result.AddProperty(PROPERTY_CREATION_DATE, result.CreationDate.Format(env.DATE_FORMAT))
	result.AddProperty(PROPERTY_ID, result.ID)
	return result
}
func (this *Item) AddProperty(aName,aValue string) *data.Property {
	result := data.Property{Name : aName, Value : aValue}
	this.Properties = append(this.Properties,result)
	return &result
}
func (this *Item) HasProperty(aName string) bool {
	for _,property := range this.Properties {
		if property.Name == aName {
			return true
		}
	}
	return false
}
func (this *Item) Read(dest []byte) (n int, err error) {
	if this.ptr >= len(this.value) {
		return 0,io.EOF
	} else {
		reste := len(this.value) - this.ptr
		count := int(math.Min(float64(reste),float64(len(dest))))
		copy(dest,this.value[this.ptr:this.ptr+count])
		this.ptr = this.ptr + count
		return count,nil
	}
}
func (this *Item) GetAge() time.Duration {
	now := time.Now()
	return now.Sub(this.CreationDate)
}
func (this *Item) Size() int {
	return len(this.value)
}
func (this *Item) Reset() {
	this.ptr = 0
}
func (this *Item) SetShared(isShared bool){
	this.shared = isShared
}
func (this *Item) IsShared() bool {
	return this.shared
}