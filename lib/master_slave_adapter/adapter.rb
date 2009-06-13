module ActiveRecord

  module ConnectionAdapters

    class MasterSlaveAdapter

      SELECT_METHODS = [ :select_all, :select_one, :select_rows, :select_value, :select_values ]

      include ActiveSupport::Callbacks
      define_callbacks :checkout, :checkin

      checkout :test_connections

      attr_accessor :connections
      attr_accessor :database_config


      delegate :select_all, :select_one, :select_rows, :select_value, :select_values, :to => :slave_connection

      def initialize( config )
        if config[:master].blank?
          raise "There is no :master config in the database configuration provided -> #{config.inspect} "
        end
        self.database_config = config
        self.connections = []
      end

      def slave_connection
        if ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.master_enabled?
          master_connection
        elsif @master_connection && @master_connection.open_transactions > 0
          master_connection
        else
          @slave_connection ||= ActiveRecord::Base.send( "#{self.database_config[:master_slave_adapter]}_connection", self.database_config.symbolize_keys )
        end
      end

      def reconnect!
        @active = true
        self.connections.each { |c| c.reconnect! }
      end

      def disconnect!
        @active = false
        self.connections.each { |c| c.disconnect! }
      end

      def reset!
        self.connections.each { |c| c.reset! }
      end

      def method_missing( name, *args, &block )
        self.master_connection.send( name.to_sym, *args, &block )
      end

      def master_connection
        @master_connection ||= ActiveRecord::Base.send( "#{self.database_config[:master_slave_adapter]}_connection", self.database_config[:master].symbolize_keys )
      end

      def connections
        [ @master_connection, @slave_connection ].compact
      end

      def test_connections
        self.connections.each do |c|
          begin
            c.select_value( 'SELECT 1', 'test select' )
          rescue
            c.reconnect!
          end
        end
      end

      class << self

        def with_master
          enable_master
          begin
            yield
          ensure
            disable_master
          end
        end

        def master_enabled?
          Thread.current[ :master_slave_enabled ]
        end

        def enable_master
          Thread.current[ :master_slave_enabled ] = true
        end

        def disable_master
          Thread.current[ :master_slave_enabled ] = nil
        end

      end

    end

  end

end