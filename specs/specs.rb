require 'rubygems'
require 'active_record'
require 'spec'

$LOAD_PATH << File.expand_path(File.join( File.dirname( __FILE__ ), '..', 'lib' ))

require 'active_record/connection_adapters/master_slave_adapter'

ActiveRecord::Base.instance_eval do

  def test_connection( config )
    config[:database] == 'slave' ? _slave : _master
  end

  def _master=( new_master )
    @_master = new_master
  end

  def _master
    @_master
  end

  def _slave=( new_slave )
    @_slave = new_slave
  end

  def _slave
    @_slave
  end

end

describe ActiveRecord::ConnectionAdapters::MasterSlaveAdapter do

  before do

    @mocked_methods = { :verify! => true, :reconnect! => true, :run_callbacks => true, :disconnect! => true }

    ActiveRecord::Base._master = mock( 'master connection', @mocked_methods.merge( :open_transactions => 0 )  )
    ActiveRecord::Base._slave = mock( 'slave connection', @mocked_methods )

    @master_connection = ActiveRecord::Base._master
    @slave_connection = ActiveRecord::Base._slave

  end

  after do
    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  describe 'with common configuration' do


    before do

      @database_setup = {
        :adapter => 'master_slave',
        :username => 'root',
        :database => 'slave',
        :master_slave_adapter => 'test',
        :master => { :username => 'root', :database => 'master' }
      }

      ActiveRecord::Base.establish_connection( @database_setup )

      [ @master_connection, @slave_connection ].each do |c|
        c.stub!( :select_value ).with( "SELECT 1", "test select" ).and_return( true )
      end

    end

    ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::SELECT_METHODS.each do |method|

      it "Should send the method '#{method}' to the slave connection" do
        @master_connection.stub!( :open_transactions ).and_return( 0 )
        @slave_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.connection.send( method, 'testing' )
      end

      it "Should send the method '#{method}' to the master connection if with_master was specified" do
        @master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_master do
          ActiveRecord::Base.connection.send( method, 'testing' )
        end
      end

      it "Should send the method '#{method}' to the master connection if there are open transactions" do
        @master_connection.stub!( :open_transactions ).and_return( 1 )
        @master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_master do
          ActiveRecord::Base.connection.send( method, 'testing' )
        end
      end

    end

    ActiveRecord::ConnectionAdapters::SchemaStatements.instance_methods.map(&:to_sym).each do |method|

      it "Should send the method '#{method}' from ActiveRecord::ConnectionAdapters::SchemaStatements to the master"  do
        @master_connection.should_receive( method ).and_return( true )
        ActiveRecord::Base.connection.send( method )
      end

    end

    (ActiveRecord::ConnectionAdapters::SchemaStatements.instance_methods.map(&:to_sym) - ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::SELECT_METHODS).each do |method|

      it "Should send the method '#{method}' from ActiveRecord::ConnectionAdapters::DatabaseStatements to the master"  do
        @master_connection.should_receive( method ).and_return( true )
        ActiveRecord::Base.connection.send( method )
      end

    end

    it 'Should be a master slave connection' do
      ActiveRecord::Base.connection.class.should == ActiveRecord::ConnectionAdapters::MasterSlaveAdapter
    end

    it 'Should have a master connection' do
      ActiveRecord::Base.connection.master_connection.should == @master_connection
    end

    it 'Should have a slave connection' do
      @master_connection.stub!( :open_transactions ).and_return( 0 )
      ActiveRecord::Base.connection.slave_connection.should == @slave_connection
    end

  end

  describe 'with connection testing disabled' do

    before do
      @database_setup = {
        :adapter => 'master_slave',
        :username => 'root',
        :database => 'slave',
        :disable_connection_test => 'true',
        :master_slave_adapter => 'test',
        :master => { :username => 'root', :database => 'master' }
      }

      ActiveRecord::Base.establish_connection( @database_setup )

    end

    ActiveRecord::ConnectionAdapters::SchemaStatements.instance_methods.map(&:to_sym).each do |method|

      it "Should not perform the testing select on the master if #{method} is called" do
        @master_connection.should_not_receive( :select_value ).with( "SELECT 1", "test select" )
        @master_connection.should_receive( method ).with('testing').and_return(true)
        ActiveRecord::Base.connection.send(method, 'testing')
      end

    end

    ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::SELECT_METHODS.each do |method|

      it "Should not perform the testing select on the slave if #{method} is called" do
        @slave_connection.should_not_receive( :select_value ).with( "SELECT 1", "test select" )
        @slave_connection.should_receive( method ).with('testing').and_return(true)
        ActiveRecord::Base.connection.send(method, 'testing')
      end

    end

  end

  describe 'with connection eager loading enabled' do

    before do
      @database_setup = {
        :adapter => 'master_slave',
        :username => 'root',
        :database => 'slave',
        :eager_load_connections => 'true',
        :master_slave_adapter => 'test',
        :master => { :username => 'root', :database => 'master' }
      }

      ActiveRecord::Base.establish_connection( @database_setup )

      [ @master_connection, @slave_connection ].each do |c|
        c.should_receive( :select_value ).with( "SELECT 1", "test select" ).and_return( true )
      end

    end

    it 'should load the master connection before any method call' do
      ActiveRecord::Base.connection.instance_variable_get(:@master_connection).should == @master_connection
    end

    it 'should load the slave connection before any method call' do
      ActiveRecord::Base.connection.instance_variable_get(:@slave_connection).should == @slave_connection
    end

  end
 
end