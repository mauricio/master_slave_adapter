ignored_methods = ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::SELECT_METHODS + ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.instance_methods
ignored_methods.uniq!
ignored_methods.map! { |v| v.to_sym }

instance_methods = ActiveRecord::ConnectionAdapters::DatabaseStatements.instance_methods + ActiveRecord::ConnectionAdapters::SchemaStatements.instance_methods
instance_methods.uniq!
instance_methods.map! { |v| v.to_sym }
instance_methods.reject! { |v| ignored_methods.include?( v ) }

instance_methods.each do |method|

  ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.class_eval %Q!

    def #{method}( *args, &block )
        self.master_connection.#{method}( *args, &block )
    end

  !

end