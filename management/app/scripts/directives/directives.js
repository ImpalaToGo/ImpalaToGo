'use strict';

var app=angular.module('impala2GoApp.directives');

/**
 * @ngdoc directive
 * @name impala2GoApp.directives:numeric
 * @description
 * # numeric input control
 */
app.directive('numeric', function(utilities,$compile) {
    return {
        require: 'ngModel',
        restrict: 'AE',
        link: function(scope, element, attrs,ngModel) {
            var numeric={},numericInput="";
             numeric=scope.$eval(attrs.numeric);
            var init = function (value) {
                element.dpNumberPicker({
                    min: 0, // Minimum value.
                    max: false, // Maximum value.
                    value: value|| 0, // Initial value
                    step: 1, // Incremental/decremental step on up/down change.
                    format: false,
                    editable: true,
                    addText: "+",
                    subText: "-",
                    afterIncrease: function(){
                        if(angular.isFunction(numeric.onIncrease)){
                            scope.$apply(function(){
                                numeric.onIncrease(numeric.price);
                            });

                        }
                    },
                    afterDecrease: function(){
                        if(angular.isFunction(numeric.onDecrease)){
                            scope.$apply(function(){
                                numeric.onDecrease(numeric.price);
                            });

                        }
                    }
                });
            }
            // Late-bind to prevent compiler clobbering
            scope.$evalAsync(function(){
                init(0);
                // add ngModel directive to numeric input
                numericInput=element.find(".dp-numberPicker-input");
                $compile(numericInput.attr("ng-model",attrs.ngModel))(scope);
            });

            // Update numeric from model value
            ngModel.$render = function () {
                scope.$evalAsync(function () {
                    init(ngModel.$viewValue);
                });
            }
            scope.$watch(attrs.ngModel, function () {
                ngModel.$render();
            });
        }
    };
});




