package viz

import "github.com/vchitepu/gopandas/lib/dataframe"

func RenderTable(df dataframe.DataFrame, th Theme, termWidth int) string {
	return df.String()
}
