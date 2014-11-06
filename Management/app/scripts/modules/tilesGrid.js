'use strict';


var app=angular.module('impala2GoApp.tilesGrid');


/**
 * @ngdoc service
 * @name impala2GoApp.tilesGrid:tilesGridCacheService
 * @description
 * # tiles Grid cache service
 */

// Set up the cache ‘tilesGrid’ service
app.factory('tilesGridCacheService', function($cacheFactory) {
    return $cacheFactory('tilesGridCache');
});

/**
 * @ngdoc directive
 * @name impala2GoApp.tilesGrid:tilesGrid
 * @description
 * # tiles Grid directive
 */
app.directive('tilesGrid', function ($compile,utilities,logger,tilesGridCacheService) {
    return {
        restrict: 'EA',
        replace:true,
        scope: true,
        compile: function() {
            return {
                pre: function(scope, iElement, iAttars) {
                    var logName="tilesGrid", gridColumn="",options,firstTimeBuildColumns=true;
                    scope.gridOptions = scope.$eval(iAttars.tilesGrid) || {};
                    options=scope.gridOptions;
                    if(angular.isString(scope.gridOptions.data)){
                        scope.$parent.$watchCollection(scope.gridOptions.data,function(val){
                            init(val);
                        });
                    }
                    else{
                        iElement.html("Data source is not string!");
                        logger.logWarning("Data source is not string!",data,logName,true);
                        //  return;
                    }

                    tilesGridCacheService.put("gridOptions",options);
                    function init(data){
                        // var gridOptions=scope.tilesGrid;
                        scope.dataArray=data;
                        if(!angular.isArray(data) || !utilities.isObject(options)){
                            iElement.html("No data!");
                            logger.logWarning("No data!",data,logName,true);
                            //  return;
                        }
                        if(firstTimeBuildColumns){
                            buildColumns(data);
                            //build columns only ones.
                            if((angular.isArray(data) && data.length) || (angular.isArray(options.columns) && options.columns.length)){
                                firstTimeBuildColumns=false;
                            }
                        }

                        //build rows
                        var gridRow="<tr ng-repeat='item in dataArray track by $index' tile-row='item' tile-columns='gridOptions.columns' controls-template='gridOptions.controlsTemplate'></tr>";

                        var gridHead="<thead><tr>"+gridColumn+"</tr></thead>";
                        var gridBody="<tbody>"+gridRow+"</tbody>";
                        var grid="<div class='table-responsive tiles-grid-container' ><table class='table table-bordered'>"+gridHead+gridBody+"</table></div>";

                        //compile table
                        var compiledHtml=$compile(grid)(scope);
                        iElement.html(compiledHtml);


                       // Dispatches an event name upwards through the scope hierarchy notifying the listeners
                        scope.$emit('tileGridUpdated', data);
                    }
                    function buildColumns(data){
                        gridColumn="";
                        if(!angular.isArray(options.columns) || !options.columns.length){
                            if(data.length){
                                options.columns=Object.keys(data[0]).map(function(item){
                                    return {name:item};
                                });
                            }
                            else{
                                iElement.html("No data!");
                                logger.logWarning("No data!",data,logName,true);
                                //  return;
                            }
                        }
                        //build columns
                        angular.forEach(options.columns,function(col){
                            var value=col.displayName|| col.name;
                            gridColumn+="<th >"+value+"</th>";
                        });
                    }
                },
                controller: function(scope){
                    this.gridOptions=scope.gridOptions;
                }

            }
        }

    };
});

/**
 * @ngdoc directive
 * @name impala2GoApp.tilesGrid:tileRow
 * @description
 * # tile row directive
 */
app.directive('tileRow',function(utilities,$compile,tilesGridCacheService){
    return {
        restrict: 'EA',
        scope:true,
        link: function postLink(scope, element, attrs) {
            scope.tileRow = scope.$eval(attrs.tileRow) || {};
            scope.tileColumns = scope.$eval(attrs.tileColumns) || {};
            scope.controlsTemplate = scope.$eval(attrs.controlsTemplate) || {};
            var gridOptions=tilesGridCacheService.get("gridOptions");
            renderRow();

            function renderRow(){
                var gridData="",template="",elementList=[];
                if(utilities.isObject(scope.tileRow) && angular.isArray(scope.tileColumns) &&  scope.tileColumns.length){
                    angular.forEach(scope.tileColumns,function(col){
                        scope.tileData=scope.tileRow[col.name] || "";
                        //  var className=col.class|| "";
                        if(scope.controlsTemplate && angular.isString(scope.controlsTemplate)){
                            template= "<div class='tile-caption'>" +scope.controlsTemplate+"</div>"
                        }
                        var gridDataHtml="<td color-state='tileData'><div class='tileContainer' ng-class='{\"tile-mask\":controlsTemplate}'><div class='tile-text' tile-data='tileData'></div>"+template+"</div></td>";
                        gridData=$compile(gridDataHtml)(scope);
                        elementList.push(gridData);

                    });
                }
                element.html(elementList);
            }
        }
    };
});

/**
 * @ngdoc directive
 * @name impala2GoApp.tilesGrid:tileData
 * @description
 * # tile data directive
 */
app.directive('tileData',function(utilities,$compile,tilesGridCacheService,$filter){
    return {
        restrict: 'EA',
        scope:true,
        link: function postLink(scope, element, attrs) {
            var order = function(data,predicate, reverse) {
                var orderBy = $filter('orderBy');
                scope.tileData = orderBy(data, predicate,reverse);
            };
            var tileData,dataList=[];
            scope.tileData = scope.$eval(attrs.tileData) || {};
            tileData=scope.tileData;
            if(utilities.isObject(tileData)){
                scope.gridOptions=tilesGridCacheService.get("gridOptions");
                scope.valueSort=scope.gridOptions.valueSort;
                if(utilities.isObject(scope.valueSort)){

                    var sign=scope.valueSort.ascending?"+":"-";
                    var predicate = scope.valueSort.predicate;
                 //   order(utilities.toArray(tileData),predicate,true);
                }
                scope.ignoreProperties=scope.gridOptions.ignoreProperties||[];
                angular.forEach(scope.gridOptions.ignoreProperties,function(prop){
                    scope.ignoreProperties[prop]=true;
                });
                //    var items=Object.keys(tileData).length-gridOptions.ignoreProperties.length;
                //   var calcWidth=Math.round(100/items);
                //     var width=calcWidth+"%";
                dataList=$compile("<span  class='tile-item' ng-repeat='(key,value) in tileData track by $index' ng-hide='ignoreProperties[key]'><span >{{value}}</span><span ng-show='gridOptions.valueSign.name==key'>{{gridOptions.valueSign.sign}}</span></span>")(scope);
                element.html(dataList);
            }
            else if(angular.isString(tileData)){
                element.html("<span class='tile-item'>"+tileData+"</span>");
            }

        }
    };
});


/**
 * @ngdoc directive
 * @name impala2GoApp.tilesGrid:colorState
 * @description
 * # tile color state directive
 */
app.directive('colorState',function(utilities,$compile,tilesGridCacheService){
    return {
        restrict: 'EA',
        scope:true,
        link: function postLink(scope, element, attrs) {
            var colorState,tileData;
            var gridOptions=tilesGridCacheService.get("gridOptions");
            colorState=gridOptions.colorState;
            scope.tileData = scope.$eval(attrs.colorState) || {};
            tileData=scope.tileData;
            if(!gridOptions || !angular.isArray(gridOptions.colorState)|| angular.isString(tileData)){
                return;
            }
            scope.$watch("tileData",function(val){
                if(utilities.isObject(val)){
                    setColor();
                }
            },true);
            function setColor(){
                if(utilities.isObject(tileData)){
                    angular.forEach(colorState,function(state){
                        var value=tileData[state.name];
                        if(value>=state.min && value<=state.max){
                            element.css({"background-color":state.background,color:state.color});
                        }
                    });
                }
            }
        }
    };
});

