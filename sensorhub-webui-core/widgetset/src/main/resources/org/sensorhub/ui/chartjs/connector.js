var org_sensorhub_ui_chartjs_instances = {};

var overlaySlider = function(domId, chart, connector) {
    var domElt = document.getElementById(domId);
    var data = chart.data.datasets[0].data;
    var t0 = Math.floor(data[0].t/60000)*60;
    var t1 = Math.ceil(data[data.length-1].t/60000)*60;
    
    function format(t) {
        return new Date(t*1000).toISOString();
    } 
    
    var slider = noUiSlider.create(domElt, {
      start: [t0, t1],
      connect: [true,true,true],
      tooltips: [{to: format}, {to: format}], // formatter
      step: 60,
      range: {
        'min': t0,
        'max': t1
      },
      behaviour: 'drag'
    });
    
    // adjust slider position so it's aligned on chart axis
    var sliderElt = slider.target.getElementsByClassName('noUi-base')[0];
    sliderElt.style.left = chart.scales['x-axis-0'].left + "px";
    sliderElt.style.width = chart.scales['x-axis-0'].width + "px";
    
    // listen for slider changes
    slider.on('change', function(e) {
        var min = parseFloat(e[0]);
        var max = parseFloat(e[1]);
        // need this check because we receive two events on segment drag
        if (!slider.hasOwnProperty('oldMin') || min != slider.oldMin || max != slider.oldMax) {
            slider.oldMin = min;
            slider.oldMax = max;
            connector.onSliderChange(min, max);
        }        
    });
}

window.org_sensorhub_ui_chartjs_Chart = function () {
    
    Chart._adapters._date.prototype.format = function(t, f) {
        //return new Date(t).toISOString();" +
        return moment(t).utc().format(f);
    }
    
    this.onStateChange = function () {
        // read state
        var newPlot = this.getState().newPlot;
        var domId = this.getState().domId;        
        var color = Chart.helpers.color;
        
        if (newPlot) {
            var data = eval(this.getState().data);
            eval(this.getState().config);
            
            // set chart context
            var canvas = document.createElement("canvas");
            document.getElementById(domId).appendChild(canvas);
            var ctx = canvas.getContext('2d');
            var chart = new Chart(canvas.getContext('2d'), config);
            org_sensorhub_ui_chartjs_instances[domId] = chart;
            
            var self = this;
            canvas.onclick = function(evt){
                var activePoints = chart.getElementsAtEvent(evt);
                console.log(activePoints);
                //self.onZoom(xRange[0], xRange[1]);
            };
            
            if (this.getState().overlaySlider) {
                overlaySlider(domId, chart, this);
            }
            
        } else {
            var data = eval(this.getState().data);
            var chart = org_sensorhub_ui_chartjs_instances[domId];
            chart.data.datasets[0].data = data;
            chart.update(0);
        }        
    };
};