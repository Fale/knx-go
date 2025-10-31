package postgres

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/knx-go/knx-go/knx/proxy"
)

type config struct {
	host     string
	port     string
	user     string
	password string
	database string
	sslMode  string
}

type Conn struct {
	netConn net.Conn
	cfg     config
	mu      sync.Mutex
}

func Dial(dsn string) (*Conn, error) {
	cfg, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}

	address := net.JoinHostPort(cfg.host, cfg.port)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres at %s: %w", address, err)
	}

	pg := &Conn{netConn: conn, cfg: cfg}
	if err := pg.startup(); err != nil {
		conn.Close()
		return nil, err
	}

	return pg, nil
}

func parseConfig(dsn string) (config, error) {
	var cfg config

	parsed, err := url.Parse(dsn)
	if err != nil {
		return cfg, fmt.Errorf("invalid postgres connection string: %w", err)
	}
	switch parsed.Scheme {
	case "postgres", "postgresql":
	default:
		return cfg, fmt.Errorf("unsupported postgres scheme %q", parsed.Scheme)
	}

	cfg.host = parsed.Hostname()
	if cfg.host == "" {
		cfg.host = "127.0.0.1"
	}

	cfg.port = parsed.Port()
	if cfg.port == "" {
		cfg.port = "5432"
	}

	if parsed.User != nil {
		cfg.user = parsed.User.Username()
		cfg.password, _ = parsed.User.Password()
	}

	if cfg.user == "" {
		return cfg, errors.New("postgres connection requires a username")
	}

	database := strings.TrimPrefix(parsed.Path, "/")
	if database == "" {
		database = cfg.user
	}
	cfg.database = database

	cfg.sslMode = parsed.Query().Get("sslmode")
	if cfg.sslMode == "" {
		cfg.sslMode = "disable"
	}
	if cfg.sslMode != "disable" {
		return cfg, fmt.Errorf("sslmode %q not supported; set sslmode=disable", cfg.sslMode)
	}

	return cfg, nil
}

func (pg *Conn) startup() error {
	if pg.cfg.sslMode != "disable" {
		return fmt.Errorf("unsupported sslmode %q", pg.cfg.sslMode)
	}

	if err := pg.sendStartupMessage(); err != nil {
		return err
	}

	for {
		typ, payload, err := pg.readMessage()
		if err != nil {
			return err
		}

		switch typ {
		case 'R':
			if err := pg.handleAuthentication(payload); err != nil {
				return err
			}
		case 'S', 'K', 'N':
			continue
		case 'E':
			return parsePostgresError(payload)
		case 'Z':
			return nil
		default:
			continue
		}
	}
}

func (pg *Conn) sendStartupMessage() error {
	var payload bytes.Buffer

	if err := binary.Write(&payload, binary.BigEndian, int32(196608)); err != nil {
		return err
	}

	writePair := func(key, value string) {
		if value == "" {
			return
		}
		payload.WriteString(key)
		payload.WriteByte(0)
		payload.WriteString(value)
		payload.WriteByte(0)
	}

	writePair("user", pg.cfg.user)
	writePair("database", pg.cfg.database)
	writePair("application_name", "knxctl2")
	writePair("client_encoding", "UTF8")
	payload.WriteByte(0)

	body := payload.Bytes()
	length := int32(len(body) + 4)

	var message bytes.Buffer
	if err := binary.Write(&message, binary.BigEndian, length); err != nil {
		return err
	}
	message.Write(body)

	_, err := pg.netConn.Write(message.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send startup message: %w", err)
	}
	return nil
}

func (pg *Conn) handleAuthentication(payload []byte) error {
	if len(payload) < 4 {
		return errors.New("invalid authentication payload")
	}

	code := binary.BigEndian.Uint32(payload[:4])
	switch code {
	case 0: // AuthenticationOk
		return nil
	case 3: // AuthenticationCleartextPassword
		if pg.cfg.password == "" {
			return errors.New("postgres server requested a password but none was provided")
		}
		return pg.sendPassword(pg.cfg.password)
	case 5: // AuthenticationMD5Password
		if len(payload) < 8 {
			return errors.New("invalid md5 authentication payload")
		}
		if pg.cfg.password == "" {
			return errors.New("postgres server requested a password but none was provided")
		}
		salt := payload[4:8]
		sum := md5.Sum([]byte(pg.cfg.password + pg.cfg.user))
		inner := fmt.Sprintf("%x", sum)
		outer := md5.Sum(append([]byte(inner), salt...))
		hashed := "md5" + fmt.Sprintf("%x", outer)
		return pg.sendPassword(hashed)
	default:
		return fmt.Errorf("unsupported authentication request: %d", code)
	}
}

func (pg *Conn) sendPassword(password string) error {
	payload := append([]byte(password), 0)
	return pg.writeMessage('p', payload)
}

func (pg *Conn) EnsureSchema() error {
	const statement = `CREATE TABLE IF NOT EXISTS knx_events (
        id BIGSERIAL PRIMARY KEY,
        event_time TIMESTAMPTZ NOT NULL,
        command TEXT NOT NULL,
        source TEXT NOT NULL,
        destination TEXT NOT NULL,
        data BYTEA,
        group_name TEXT,
        dpt TEXT,
        decoded TEXT
);`
	return pg.simpleQuery(statement)
}

func (pg *Conn) InsertEvent(ctx context.Context, event proxy.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	timestamp := event.Timestamp.UTC().Format(time.RFC3339Nano)

	eventTime := quoteLiteral(timestamp) + "::timestamptz"
	command := quoteLiteral(event.Command)
	sourceValue := strings.TrimSpace(event.Source)
	if sourceValue == "" {
		sourceValue = "0.0.0"
	}
	source := quoteLiteral(sourceValue)
	destination := quoteLiteral(event.Destination)

	data := "NULL"
	if len(event.Data) > 0 {
		hexData := hex.EncodeToString(event.Data)
		data = fmt.Sprintf("E'\\\\x%s'", hexData)
	}

	groupValue := nullableLiteral(event.Group)
	dptValue := nullableLiteral(event.DPT)
	decodedValue := nullableLiteral(event.Decoded)

	query := fmt.Sprintf(
		"INSERT INTO knx_events (event_time, command, source, destination, data, group_name, dpt, decoded) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
		eventTime,
		command,
		source,
		destination,
		data,
		groupValue,
		dptValue,
		decodedValue,
	)

	return pg.simpleQuery(query)
}

func (pg *Conn) SelectEvents(ctx context.Context, destination string, limit int) ([]proxy.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(
		"SELECT event_time, command, source, destination, data, group_name, dpt, decoded FROM knx_events WHERE destination = %s ORDER BY event_time DESC LIMIT %d;",
		quoteLiteral(destination),
		limit,
	)

	rows, err := pg.simpleQueryRows(query)
	if err != nil {
		return nil, err
	}

	events := make([]proxy.Event, 0, len(rows))
	for _, row := range rows {
		event, err := rowToEvent(row)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

func nullableLiteral(value string) string {
	if strings.TrimSpace(value) == "" {
		return "NULL"
	}
	return quoteLiteral(value)
}

func quoteLiteral(value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

func (pg *Conn) simpleQuery(query string) error {
	_, err := pg.simpleQueryRows(query)
	return err
}

func (pg *Conn) simpleQueryRows(query string) ([]map[string][]byte, error) {
	pg.mu.Lock()
	defer pg.mu.Unlock()

	if err := pg.writeMessage('Q', append([]byte(query), 0)); err != nil {
		return nil, err
	}

	var (
		columns []string
		rows    []map[string][]byte
	)

	for {
		typ, payload, err := pg.readMessage()
		if err != nil {
			return nil, err
		}

		switch typ {
		case 'E':
			return nil, parsePostgresError(payload)
		case 'T':
			cols, err := parseRowDescription(payload)
			if err != nil {
				return nil, err
			}
			columns = cols
		case 'D':
			if len(columns) == 0 {
				return nil, errors.New("received data row without description")
			}
			row, err := parseDataRow(payload, columns)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		case 'C':
			columns = nil
		case 'Z':
			return rows, nil
		default:
			continue
		}
	}
}

func parseRowDescription(payload []byte) ([]string, error) {
	reader := bytes.NewReader(payload)

	var fieldCount uint16
	if err := binary.Read(reader, binary.BigEndian, &fieldCount); err != nil {
		return nil, err
	}

	columns := make([]string, fieldCount)
	for i := 0; i < int(fieldCount); i++ {
		name, err := readCString(reader)
		if err != nil {
			return nil, err
		}
		columns[i] = name

		if _, err := reader.Seek(18, io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	return columns, nil
}

func parseDataRow(payload []byte, columns []string) (map[string][]byte, error) {
	reader := bytes.NewReader(payload)

	var fieldCount uint16
	if err := binary.Read(reader, binary.BigEndian, &fieldCount); err != nil {
		return nil, err
	}
	if int(fieldCount) != len(columns) {
		return nil, errors.New("column count mismatch")
	}

	row := make(map[string][]byte, fieldCount)
	for i := 0; i < int(fieldCount); i++ {
		var length int32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return nil, err
		}

		if length == -1 {
			row[columns[i]] = nil
			continue
		}

		value := make([]byte, length)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		row[columns[i]] = value
	}

	return row, nil
}

func readCString(r *bytes.Reader) (string, error) {
	var buf []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0 {
			return string(buf), nil
		}
		buf = append(buf, b)
	}
}

func rowToEvent(row map[string][]byte) (proxy.Event, error) {
	var event proxy.Event

	if raw := row["event_time"]; raw != nil {
		ts, err := parseTimestamp(string(raw))
		if err != nil {
			return event, err
		}
		event.Timestamp = ts.UTC()
	}
	if raw := row["command"]; raw != nil {
		event.Command = string(raw)
	}
	if raw := row["source"]; raw != nil {
		event.Source = string(raw)
	}
	if raw := row["destination"]; raw != nil {
		event.Destination = string(raw)
	}
	if raw := row["data"]; raw != nil {
		data, err := parseBytea(raw)
		if err != nil {
			return event, err
		}
		event.Data = data
	}
	if raw := row["group_name"]; raw != nil {
		event.Group = string(raw)
	}
	if raw := row["dpt"]; raw != nil {
		event.DPT = string(raw)
	}
	if raw := row["decoded"]; raw != nil {
		event.Decoded = string(raw)
	}

	return event, nil
}

func parseTimestamp(value string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05-07",
		time.RFC3339Nano,
	}

	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp %q", value)
}

func parseBytea(value []byte) ([]byte, error) {
	if len(value) >= 2 && value[0] == '\\' && value[1] == 'x' {
		return hex.DecodeString(string(value[2:]))
	}
	if len(value) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported bytea encoding")
}

func (pg *Conn) writeMessage(typ byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)+4))

	if _, err := pg.netConn.Write(header); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	if _, err := pg.netConn.Write(payload); err != nil {
		return err
	}
	return nil
}

func (pg *Conn) readMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(pg.netConn, header); err != nil {
		return 0, nil, err
	}

	typ := header[0]
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 {
		return 0, nil, errors.New("invalid message length")
	}

	payload := make([]byte, length-4)
	if _, err := io.ReadFull(pg.netConn, payload); err != nil {
		return 0, nil, err
	}

	return typ, payload, nil
}

func parsePostgresError(payload []byte) error {
	fields := map[byte]string{}
	rest := payload
	for len(rest) > 0 {
		fieldType := rest[0]
		if fieldType == 0 {
			break
		}
		rest = rest[1:]
		terminator := bytes.IndexByte(rest, 0)
		if terminator < 0 {
			break
		}
		fields[fieldType] = string(rest[:terminator])
		rest = rest[terminator+1:]
	}

	message := fields['M']
	if message == "" {
		message = "postgres error"
	}
	if detail := fields['D']; detail != "" {
		message = message + ": " + detail
	}

	return errors.New(message)
}

func (pg *Conn) Close() {
	if pg == nil || pg.netConn == nil {
		return
	}

	pg.writeMessage('X', nil)
	pg.netConn.Close()
	pg.netConn = nil
}
