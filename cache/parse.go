package cache

import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/httpx"

//PageData is exported
type PageData struct {
	PageSize  int `json:"pageSize"`
	PageIndex int `json:"pageIndex"`
	TotalRows int `json:"total_rows"`
}

//LocationsNameResponse is exported
type LocationsNameResponse struct {
	PageData
	Names []string
}

func parseLocationsNameResponse(respData *httpx.HttpResponse) (*LocationsNameResponse, error) {

	data := struct {
		PageData
		Rows []struct {
			Location string `json:"location"`
		} `json:"rows"`
	}{}

	if err := respData.JSON(&data); err != nil {
		return nil, err
	}

	response := &LocationsNameResponse{
		PageData: data.PageData,
		Names:    []string{},
	}

	for _, row := range data.Rows {
		response.Names = append(response.Names, row.Location)
	}
	return response, nil
}

//JobsResponse is exported
type JobsResponse struct {
	PageData
	Jobs []*models.Job
}

func parseJobsResponse(respData *httpx.HttpResponse) (*JobsResponse, error) {

	data := struct {
		PageData
		Rows []*models.Job `json:"rows"`
	}{}

	if err := respData.JSON(&data); err != nil {
		return nil, err
	}

	return &JobsResponse{
		PageData: data.PageData,
		Jobs:     data.Rows,
	}, nil
}

//SimpleJobsResponse is exported
type SimpleJobsResponse struct {
	PageData
	Jobs []*models.SimpleJob
}

func parseSimpleJobsResponse(respData *httpx.HttpResponse) (*SimpleJobsResponse, error) {

	data := struct {
		PageData
		Rows []*models.SimpleJob `json:"rows"`
	}{}

	if err := respData.JSON(&data); err != nil {
		return nil, err
	}

	return &SimpleJobsResponse{
		PageData: data.PageData,
		Jobs:     data.Rows,
	}, nil
}
