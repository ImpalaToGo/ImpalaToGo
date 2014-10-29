'use strict';


var app=angular.module('impala2GoApp.tilesGrid');

/**
 * @ngdoc directive
 * @name impala2GoApp.tilesGrid:tilesGrid
 * @description
 * # tiles Grid directive
 */
app.directive('tilesGrid', function ($compile,utilities,logger) {
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


                        logger.log("Tile Grid updated","",logName,true);
                        //rgba(255, 0, 0, 0.97)
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
app.directive('tileRow',function(utilities,$compile){
    return {
        restrict: 'EA',
        scope:true,
        link: function postLink(scope, element, attrs) {
            scope.tileRow = scope.$eval(attrs.tileRow) || {};
            scope.tileColumns = scope.$eval(attrs.tileColumns) || {};
            scope.controlsTemplate = scope.$eval(attrs.controlsTemplate) || {};
            renderRow();
            //render row on object change
            scope.$watch("tileRow", function (newVal) {
                if(newVal){
                    renderRow();
                }
            },true);

            //build td
            function renderRow(){
                var gridData="",template="",captionClass="";
                if(utilities.isObject(scope.tileRow) && angular.isArray(scope.tileColumns) &&  scope.tileColumns.length){
                    angular.forEach(scope.tileColumns,function(col){
                        var value=scope.tileRow[col.name] || "";
                        //  var className=col.class|| "";
                        if(scope.controlsTemplate && angular.isString(scope.controlsTemplate)){
                            template= "<div class='tile-caption'>" +scope.controlsTemplate+"</div>"
                            captionClass="tile-mask";
                        }
                        gridData+="<td  class='"+captionClass+"'><span>"+value+"</span>"+template+"</td>";
                    });
                }
                element.html($compile(gridData)(scope));
            }
        }
    };
});

/**
 * @ngdoc directive
 * @name impala2GoApp.tilesGrid:jTile
 * @description
 * # tile control directive
 */
//app.directive('jTile',function(utilities){
//    return {
//        restrict: 'EA',
//        scope:true,
//        link: function postLink(scope, element, attrs) {
//
//            scope.$evalAsync(function(){
//              //  $(element).jTile();
//            });
//        }
//    };
//});

