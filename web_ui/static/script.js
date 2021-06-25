function annotate_heatmap(layout, xValues, yValues, zValues) {
    for (var i = 0; i < yValues.length; i++) {
        for (var j = 0; j < xValues.length; j++) {
            //var currentValue = zValues[i][j];
            var textColor = 'white';
            /*if (currentValue != 0.0) {
                var textColor = 'white';
            } else {
                var textColor = 'black';
            }*/
            var result = {
                xref: 'x1',
                yref: 'y1',
                x: xValues[j],
                y: yValues[i],
                text: ((zValues[i][j] != 0) ? parseFloat(zValues[i][j]).toFixed(2) : ""),
                font: {
                    family: 'Arial',
                    size: 12,
                    color: 'rgb(50, 171, 96)'
                },
                showarrow: false,
                font: {
                    color: textColor
                }
            };
            layout.annotations.push(result);
        }
    }
    return layout
}