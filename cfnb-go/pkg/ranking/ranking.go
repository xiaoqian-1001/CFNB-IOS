package ranking

import "sort"

type ScoredNode struct {
	Node      string
	Score     float64
	Speed     float64
	TCPLat    float64
	HTTPLat   float64
}

func ScoreAndRank(bwResults []ScoredNodeInput, latencyMap map[string]float64, httpLatencyMap map[string]float64, httpJitterMap map[string]float64, speedWeight, tcpLatencyWeight, httpLatencyWeight, jitterWeight float64) []ScoredNode {
	scored := make([]ScoredNode, 0)

	for _, item := range bwResults {
		tcpLat := 999.0
		if v, ok := latencyMap[item.Node]; ok {
			tcpLat = v
		}

		httpLat := 999999.0
		if v, ok := httpLatencyMap[item.Node]; ok {
			httpLat = v
		}

		httpJitter := 999999.0
		if v, ok := httpJitterMap[item.Node]; ok {
			httpJitter = v
		}

		httpLatSec := httpLat / 1000.0
		httpJitterSec := httpJitter / 1000.0
		penalty := 1.0 + tcpLatencyWeight*tcpLat + httpLatencyWeight*httpLatSec + jitterWeight*httpJitterSec
		score := (speedWeight * item.Speed) / penalty

		scored = append(scored, ScoredNode{
			Node:    item.Node,
			Score:   score,
			Speed:   item.Speed,
			TCPLat:  tcpLat,
			HTTPLat: httpLat,
		})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	return scored
}

type ScoredNodeInput struct {
	Node  string
	Speed float64
}

func SelectGlobal(scored []ScoredNode, topN int) []string {
	result := make([]string, 0)
	for i, s := range scored {
		if i >= topN {
			break
		}
		result = append(result, s.Node)
	}
	return result
}

func SelectPerCountry(scored []ScoredNode, topN int) []string {
	countryMap := make(map[string][]ScoredNode)
	for _, s := range scored {
		country := ""
		if idx := findLast(s.Node, '#'); idx >= 0 {
			country = s.Node[idx+1:]
			if spaceIdx := findFirst(country, ' '); spaceIdx >= 0 {
				country = country[:spaceIdx]
			}
		}
		if country == "" {
			country = "__unknown__"
		}
		countryMap[country] = append(countryMap[country], s)
	}

	scoreMap := make(map[string]float64)
	for _, s := range scored {
		scoreMap[s.Node] = s.Score
	}

	result := make([]string, 0)
	for _, items := range countryMap {
		sort.Slice(items, func(i, j int) bool {
			return items[i].Score > items[j].Score
		})
		for i, item := range items {
			if i >= topN {
				break
			}
			result = append(result, item.Node)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return scoreMap[result[i]] > scoreMap[result[j]]
	})

	return result
}

func findLast(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func findFirst(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
