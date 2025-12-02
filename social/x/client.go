package x

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"

	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/ocp-server/metrics"
)

const (
	baseUrl = "https://api.x.com/2/"

	metricsStructName = "social.x.client"
)

type Client struct {
	httpClient *http.Client
}

// NewClient returns a new X client
func NewClient() *Client {
	return &Client{
		httpClient: http.DefaultClient,
	}
}

// User represents the structure for a user in the X API response
type User struct {
	ID              string        `json:"id"`
	Username        string        `json:"username"`
	Name            string        `json:"name"`
	Description     string        `json:"description"`
	ProfileImageUrl string        `json:"profile_image_url"`
	VerifiedType    string        `json:"verified_type"`
	PublicMetrics   PublicMetrics `json:"public_metrics"`
}

// PublicMetrics represents the structure for public metrics in the X API response
type PublicMetrics struct {
	FollowersCount int `json:"followers_count"`
	FollowingCount int `json:"following_count"`
	TweetCount     int `json:"tweet_count"`
	LikeCount      int `json:"like_count"`
}

// GetMyUser gets the X user associated with the provided access token
func (c *Client) GetMyUser(ctx context.Context, accessToken string) (*User, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetMyUser")
	defer tracer.End()

	res, err := c.getUser(ctx, baseUrl+"users/me", accessToken)
	tracer.OnError(err)
	return res, err
}

func (c *Client) getUser(ctx context.Context, fromUrl string, accessToken string) (*User, error) {
	req, err := http.NewRequest("GET", fromUrl+"?user.fields=description,profile_image_url,public_metrics,verified_type", nil)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)

	req.Header.Add("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected http status code: %d", resp.StatusCode)
	}

	var result struct {
		Data   *User     `json:"data"`
		Errors []*xError `json:"errors"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if len(result.Errors) > 0 {
		return nil, result.Errors[0].toError()
	}
	return result.Data, nil
}

type xError struct {
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func (e *xError) toError() error {
	return errors.Errorf("%s: %s", e.Title, e.Detail)
}

func (u *User) ToProto() *profilepb.XProfile {
	return &profilepb.XProfile{
		Id:            u.ID,
		Username:      u.Username,
		Name:          u.Name,
		Description:   u.Description,
		ProfilePicUrl: u.ProfileImageUrl,
		VerifiedType:  toProtoVerifiedType(u.VerifiedType),
		FollowerCount: uint32(u.PublicMetrics.FollowersCount),
	}
}

func toProtoVerifiedType(value string) profilepb.XProfile_VerifiedType {
	switch value {
	case "blue":
		return profilepb.XProfile_BLUE
	case "business":
		return profilepb.XProfile_BUSINESS
	case "government":
		return profilepb.XProfile_GOVERNMENT
	default:
		return profilepb.XProfile_NONE
	}
}
