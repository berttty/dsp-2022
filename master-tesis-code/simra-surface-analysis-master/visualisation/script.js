L.Control.Slider = L.Control.extend({
  update: function (value) {
    return value;
  },

  options: {
    size: '100px',
    position: 'topright',
    min: 0,
    max: 250,
    step: 1,
    id: "slider",
    value: 50,
    collapsed: true,
    title: 'Leaflet Slider',
    logo: 'S',
    orientation: 'horizontal',
    increment: false,
    getValue: function (value) {
      return value;
    },
    showValue: true,
    syncSlider: false
  },
  initialize: function (f, options) {
    L.setOptions(this, options);
    if (typeof f == "function") {
      this.update = f;
    } else {
      this.update = function (value) {
        console.log(value);
      };
    }
    if (typeof this.options.getValue != "function") {
      this.options.getValue = function (value) {
        return value;
      };
    }
    if (this.options.orientation != 'vertical') {
      this.options.orientation = 'horizontal';
    }
  },
  onAdd: function (map) {
    this._initLayout();
    this.update(this.options.value + "");
    return this._container;
  },
  _updateValue: function () {
    this.value = this.slider.value;
    if (this.options.showValue) {
      this._sliderValue.innerHTML = this.options.getValue(this.value);
    }
    this.update(this.value);
  },
  _initLayout: function () {
    var className = 'leaflet-control-slider';
    this._container = L.DomUtil.create('div', className + ' ' + className + '-' + this.options.orientation);
    this._sliderLink = L.DomUtil.create('a', className + '-toggle', this._container);
    this._sliderLink.setAttribute("title", this.options.title);
    this._sliderLink.innerHTML = this.options.logo;

    if (this.options.showValue) {
      this._sliderValue = L.DomUtil.create('p', className + '-value', this._container);
      this._sliderValue.innerHTML = this.options.getValue(this.options.value);
    }

    if (this.options.increment) {
      this._plus = L.DomUtil.create('a', className + '-plus', this._container);
      this._plus.innerHTML = "+";
      L.DomEvent.on(this._plus, 'click', this._increment, this);
      L.DomUtil.addClass(this._container, 'leaflet-control-slider-incdec');
    }

    this._sliderContainer = L.DomUtil.create('div', 'leaflet-slider-container', this._container);
    this.slider = L.DomUtil.create('input', 'leaflet-slider', this._sliderContainer);
    if (this.options.orientation == 'vertical') { this.slider.setAttribute("orient", "vertical"); }
    this.slider.setAttribute("title", this.options.title);
    this.slider.setAttribute("id", this.options.id);
    this.slider.setAttribute("type", "range");
    this.slider.setAttribute("min", this.options.min);
    this.slider.setAttribute("max", this.options.max);
    this.slider.setAttribute("step", this.options.step);
    this.slider.setAttribute("value", this.options.value);
    if (this.options.syncSlider) {
      L.DomEvent.on(this.slider, "input", function (e) {
        this._updateValue();
      }, this);
    } else {
      L.DomEvent.on(this.slider, "change", function (e) {
        this._updateValue();
      }, this);
    }

    if (this.options.increment) {
      this._minus = L.DomUtil.create('a', className + '-minus', this._container);
      this._minus.innerHTML = "-";
      L.DomEvent.on(this._minus, 'click', this._decrement, this);
    }

    if (this.options.showValue) {
      if (window.matchMedia("screen and (-webkit-min-device-pixel-ratio:0)").matches && this.options.orientation == 'vertical') { this.slider.style.width = (this.options.size.replace('px', '') - 36) + 'px'; this._sliderContainer.style.height = (this.options.size.replace('px', '') - 36) + 'px'; }
      else if (this.options.orientation == 'vertical') { this._sliderContainer.style.height = (this.options.size.replace('px', '') - 36) + 'px'; }
      else { this._sliderContainer.style.width = (this.options.size.replace('px', '') - 56) + 'px'; }
    } else {
      if (window.matchMedia("screen and (-webkit-min-device-pixel-ratio:0)").matches && this.options.orientation == 'vertical') { this.slider.style.width = (this.options.size.replace('px', '') - 10) + 'px'; this._sliderContainer.style.height = (this.options.size.replace('px', '') - 10) + 'px'; }
      else if (this.options.orientation == 'vertical') { this._sliderContainer.style.height = (this.options.size.replace('px', '') - 10) + 'px'; }
      else { this._sliderContainer.style.width = (this.options.size.replace('px', '') - 25) + 'px'; }
    }

    L.DomEvent.disableClickPropagation(this._container);

    if (this.options.collapsed) {
      if (!L.Browser.android) {
        L.DomEvent
          .on(this._container, 'mouseenter', this._expand, this)
          .on(this._container, 'mouseleave', this._collapse, this);
      }

      if (L.Browser.touch) {
        L.DomEvent
          .on(this._sliderLink, 'click', L.DomEvent.stop)
          .on(this._sliderLink, 'click', this._expand, this);
      } else {
        L.DomEvent.on(this._sliderLink, 'focus', this._expand, this);
      }
    } else {
      this._expand();
    }
  },
  _expand: function () {
    L.DomUtil.addClass(this._container, 'leaflet-control-slider-expanded');
  },
  _collapse: function () {
    L.DomUtil.removeClass(this._container, 'leaflet-control-slider-expanded');
  },
  _increment: function () {
    console.log(this.slider.value - this.slider.step + " " + this.slider.value + this.slider.step);
    this.slider.value = this.slider.value * 1 + this.slider.step * 1;
    this._updateValue();
  },
  _decrement: function () {
    console.log(this.slider.value - this.slider.step + " " + this.slider.value + this.slider.step);
    this.slider.value = this.slider.value * 1 - this.slider.step * 1;
    this._updateValue();
  }


});
L.control.slider = function (f, options) {
  return new L.Control.Slider(f, options);
};

// TODO: move to leavlet overlay control
// TDOO: add  slider to configure count https://github.com/Eclipse1979/leaflet-slider
// https://digital-geography.com/filter-leaflet-maps-slider/
var map = L.map('map').setView([52.5229, 13.4130], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap contributors</a>'
}).addTo(map);

/*
var mapLink = '<a href="http://www.esri.com/">Esri</a>';
var wholink = 'i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community';

L.tileLayer(
    'http://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: '&copy; '+mapLink+', '+wholink,
    maxZoom: 18,
    }).addTo(map);
*/

function getColor(x) {
  switch (x) {
    case 1:
      return '#0dff00';
    case 2:
      return '#CCFF33';
    case 3:
      return '#FFFF00';
    case 4:
      return '#FF9900';
    case 5:
      return '#FF0000';
  }
}

var addLayer = L.FeatureGroup.prototype.addLayer;
L.GeoJSON.prototype.addLayer = function (layer) {
  return addLayer.call(this, layer);
};

var legend = L.control({ position: 'bottomleft' });
legend.onAdd = function (map) {

  var div = L.DomUtil.create('div', 'info-legend');
  labels = ['<strong>Categories</strong>'],
    categories = ['A', 'B', 'C', 'D', 'E'];

  for (var i = 1; i <= 5; i++) {

    div.innerHTML +=
      labels.push(
        '<i class="circle" style="background:' + getColor(i) + ';"></i> ' +
        (categories[i - 1] ? categories[i - 1] : '+'));
  }

  div.innerHTML = labels.join('<br>');
  div.innerHTML += "<br><button onclick=\"toggleData();\">toggle data layer</button>";
  return div;
};
legend.addTo(map);

window.countFilter = 0;

slider = L.control.slider(function (value) {
  window.countFilter = value;
  hideData();
  showData();
}, {
  max: 100,
  value: 0,
  step: 1,
  size: '250px',
  orientation: 'vertical',
  id: 'slider'
}).addTo(map);


function displayGeoJSON(geoString) {
  window.data = JSON.parse(geoString);
  window.toggle = false;
  showData();
}

function handleFile(file) {
  var reader = new FileReader();
  reader.onload = function (e) {
    displayGeoJSON(e.target.result);
  };
  reader.onerror = function (e) {
    console.error('reading failed');
  };
  reader.readAsText(file);
}

function toggleData() {
  if (!toggle) {
    hideData();
  } else {
    showData();
  }
  toggle = !toggle;
}

function showData() {
  if (window.data) {
    // deep copy
    geojson = JSON.parse(JSON.stringify(data));
    if (countFilter > 0) {
      geojson.features = geojson.features.filter(feature => feature.properties.count >= countFilter);
    }
    window.dataLayer = L.geoJson(geojson, {
      style: function (feature) {
        return {
          "fillOpacity": 1,
          "fillColor": getColor(feature.properties.result),
          "color": getColor(feature.properties.result),
          "opacity": 1,
          "weight": 3,
        };
      }
    }).addTo(map);
  }
}

function hideData() {
  if (window.dataLayer) {
    map.removeLayer(dataLayer);
  }
}


document.addEventListener("dragover", function (event) {
  event.preventDefault();
});


document.addEventListener("drop", function (event) {
  event.preventDefault();
  document.getElementById("map").style.color = "";
  event.target.style.border = "";
  var files = event.dataTransfer.files;
  if (files.length) {
    for (var i = 0; i < files.length; i++) {
      handleFile(files[i]);
    }
  }
});