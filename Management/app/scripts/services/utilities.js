'use strict';

/**
 * @ngdoc service
 * @name impala2GoApp.utilities
 * @description
 * # utilities
 * Service in the impala2GoApp.
 */
angular.module('impala2GoApp.services')
    .factory('utilities', function utilities() {
        var createGuidFn=function(){
            return  'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });
        }
        var isObjectEmptyFn =function(obj){
            for(var prop in obj) {
                if(obj.hasOwnProperty(prop))
                    return false;
            }

            return true;
        }
        var isObjectFn=function(obj){
            return typeof(obj)=="object";
        }
        var getPercentageFn=function(val){
            if(angular.isNumber(val)){
                return val/100;
            }
            return 0;
        }
        return{
            createGuid:createGuidFn,
            getPercentage:getPercentageFn,
            isObjectEmpty:isObjectEmptyFn,
            isObject:isObjectFn
        }
    });
