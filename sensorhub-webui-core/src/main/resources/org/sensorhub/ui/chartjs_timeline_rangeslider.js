var config = {
  type: 'line',
  data: {
    datasets: [{
      //backgroundColor: '#197de1aa',
      backgroundColor: '#ccccccaa',
      //borderColor: '#197de1aa',
      borderColor: '#222222aa',
      data: data,
      type: 'bar',
      //pointRadius: 0,
      fill: false,
      //lineTension: 0.5,
      //steppedLine: 'middle',
      //borderWidth: 2
      borderWidth: 1
    }]
  },
  options: {
    maintainAspectRatio: false,
    legend: {
      display: false
    },
    tooltips: {
      enabled: false
    },
    scales: {
      xAxes: [{
        type: 'time',
        bounds: 'data',
        //offset: true,
        time: {
            displayFormats: {
                hour: 'hh:mm'
            }
        },
        distribution: 'linear',
        barPercentage: 1.0,
        categoryPercentage: 0.8,
        ticks: {
          //display: false,
          source: 'auto',
          autoSkip: true,
          maxRotation: 0,
          callback: function(value, index, values) {
              if (values.length > 0 && index == 0)
                  return moment(values[0].value).format('MMM D');
              return value;
          }
        },
        gridLines: {
          display: true,
          tickMarkLength: 8
        }
      }],
      yAxes: [{
        bounds: 'data',
        display: false,
        ticks: {
          beginAtZero: true
        }
      }]
    }
  }
}