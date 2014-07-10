require 'sample/sample/entity'
register Sinatra::RbbtRESTEntity

Sample.module_eval do
  include Entity::REST
end


