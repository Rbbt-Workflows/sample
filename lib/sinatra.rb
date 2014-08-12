require 'rbbt/entity/sample'
register Sinatra::RbbtRESTEntity

Sample.module_eval do
  include Entity::REST
end


