require 'rbbt/entity/sample'
register Sinatra::RbbtRESTEntity

Sample.module_eval do
  include Entity::REST

  export_asynchronous :genomic_mutations
  export_asynchronous :mutation_genes
end


