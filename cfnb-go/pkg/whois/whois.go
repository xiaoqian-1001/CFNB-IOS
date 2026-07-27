package whois

import (
	"bufio"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"
)

var knownProviders = map[string]string{
	"XTOM":       "xTom",
	"XT OM":      "xTom",
	"KIRINO":     "Kirino",
	"DMIT":       "DMIT",
	"VOLONET":    "Volonet",
	"VOLO":       "Volonet",
	"GOMAMI":     "GoMami",
	"GO MAMI":    "GoMami",
	"MISAKA":     "Misaka",
	"AKARI":      "Akari",
	"VIRMACH":    "VirMach",
	"BUYVM":      "BuyVM",
	"RACKNERD":   "RackNerd",
	"HOSTHATCH":  "HostHatch",
	"HETZNER":    "Hetzner",
	"OVH":        "OVH",
	"VULTR":      "Vultr",
	"DIGITALOCEAN": "DigitalOcean",
	"LINODE":     "Linode",
	"AMAZON":     "AWS",
	"AWS":        "AWS",
	"GOOGLE":     "Google",
	"MICROSOFT":  "Azure",
	"AZURE":      "Azure",
	"CLOUDFLARE": "Cloudflare",
	"AKAMAI":     "Akamai",
}

var rirNames = map[string]bool{
	"asia pacific network information centre": true,
	"ripe network coordination centre":        true,
	"american registry for internet numbers":  true,
	"african network information centre":      true,
	"afrinic":                                 true,
	"latin american and caribbean ip address regional registry": true,
	"lacnic": true,
}

var orgRe = regexp.MustCompile(`(?i)^(?:org-name|OrgName|owner|Owner):\s*(.+)`)
var netRe = regexp.MustCompile(`(?i)^(?:netname|NetName):\s*(.+)`)
var geofeedRe = regexp.MustCompile(`(?i)^geofeed:\s*https?://([^/\s]+)`)

func Lookup(ip string) string {
	if runtime.GOOS == "ios" {
		return lookupTCP(ip)
	}
	name := lookupSystem(ip)
	if name != "" {
		return name
	}
	return lookupTCP(ip)
}

func lookupSystem(ip string) string {
	cmd := exec.Command("whois", ip)
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	return parseWhois(string(output))
}

func lookupTCP(ip string) string {
	whoisServer := findWhoisServer(ip)
	if whoisServer == "" {
		return ""
	}
	conn, err := net.DialTimeout("tcp", whoisServer+":43", 10*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "%s\r\n", ip)
	var lines []string
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return parseWhois(strings.Join(lines, "\n"))
}

func findWhoisServer(ip string) string {
	conn, err := net.DialTimeout("tcp", "whois.iana.org:43", 10*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "%s\r\n", ip)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "whois.") {
			parts := strings.Fields(line)
			for _, p := range parts {
				if strings.Contains(p, "whois.") {
					return strings.TrimSpace(p)
				}
			}
		}
		// Sometimes the whois server is in format: "whois:        whois.arin.net"
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "whois:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

func parseWhois(text string) string {
	for _, line := range strings.Split(text, "\n") {
		m := orgRe.FindStringSubmatch(line)
		if m != nil {
			cand := strings.TrimSpace(m[1])
			if !rirNames[strings.ToLower(cand)] {
				return normalizeName(cand)
			}
		}
	}
	for _, line := range strings.Split(text, "\n") {
		m := geofeedRe.FindStringSubmatch(line)
		if m != nil {
			domain := m[1]
			parts := strings.Split(strings.ToLower(domain), ".")
			if len(parts) >= 2 {
				idx := len(parts) - 2
				if parts[idx] == "com" || parts[idx] == "net" || parts[idx] == "org" || parts[idx] == "co" {
					idx = len(parts) - 3
				}
				if idx >= 0 {
					return strings.Title(parts[idx])
				}
			}
			return parts[0]
		}
	}
	for _, line := range strings.Split(text, "\n") {
		m := netRe.FindStringSubmatch(line)
		if m != nil {
			return normalizeName(strings.TrimSpace(m[1]))
		}
	}
	return ""
}

func normalizeName(name string) string {
	upper := strings.ToUpper(name)
	for key, val := range knownProviders {
		if strings.Contains(upper, key) {
			return val
		}
	}
	name = regexp.MustCompile(`[-_]\d{1,3}[-_]\d{1,3}[-_]\d{1,3}[-_]\d{1,3}.*$`).ReplaceAllString(name, "")
	name = regexp.MustCompile(`[-_]\d{4,}.*$`).ReplaceAllString(name, "")
	name = regexp.MustCompile(`^NET[-_]`).ReplaceAllString(name, "")
	name = strings.TrimSpace(name)
	if name == "" {
		return "Net"
	}
	if regexp.MustCompile(`^[\d\-_\.]+$`).MatchString(name) {
		return name
	}
	parts := regexp.MustCompile(`[-_\s]+`).Split(name, -1)
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.Title(strings.ToLower(p))
		}
	}
	return strings.Join(parts, " ")
}