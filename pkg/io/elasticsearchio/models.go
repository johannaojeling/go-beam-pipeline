package elasticsearchio

import "encoding/json"

type SearchRequest struct {
	Query       json.RawMessage `json:"query"`
	Pit         *PointInTime    `json:"pit,omitempty"`
	SearchAfter []any           `json:"search_after,omitempty"`
}

type OpenPITResponse struct {
	Id string `json:"id"`
}

type PointInTime struct {
	Id        string `json:"id"`
	KeepAlive string `json:"keep_alive"`
}

type SearchResponse struct {
	PitId string `json:"pit_id"`
	Hits  *Hits  `json:"hits"`
}

type Hits struct {
	Hits  []*Hit `json:"hits"`
	Total Total  `json:"total"`
}

type Hit struct {
	Source json.RawMessage `json:"_source"`
	Sort   []any           `json:"sort"`
}

type Total struct {
	Value int `json:"value"`
}
