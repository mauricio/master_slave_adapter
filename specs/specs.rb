require 'rubygems'
require 'active_record'
require 'spec'

$LOAD_PATH << File.expand_path(File.join( File.dirname( __FILE__ ), '..', 'lib' ))

require 'active_record/connection_adapters/master_slave_adapter'

ActiveRecord::Base.instance_eval do

  def test_connection( config )
    config = config.symbolize_keys

    config[:master_slave_adapter] ? _slave : _master
  end

  def _master=( new_master )
    @master = new_master
  end

  def _master
    @master
  end

  def _slave=( new_slave )
    @slave = new_slave
  end

  def _slave
    @slave
  end

end

describe ActiveRecord::ConnectionAdapters::MasterSlaveAdapter do

  before(:all) do

    @mocked_methods = { :verify! => true, :reconnect! => true, :run_callbacks => true, :disconnect! => true }

    ActiveRecord::Base._master = mock( 'master connection', @mocked_methods.merge( :open_transactions => 0 )  )
    ActiveRecord::Base._slave = mock( 'slave connection', @mocked_methods )

    @master_connection = ActiveRecord::Base._master
    @slave_connection = ActiveRecord::Base._slave

    @database_setup = {
      :adapter => 'master_slave',
      :username => 'root',
      :database => 'master_slave_test',
      :master_slave_adapter => 'test',
      :master => { :username => 'root', :database => 'master_slave_test' }
    }

    ActiveRecord::Base.establish_connection( @database_setup )

  end

  ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::SELECT_METHODS.each do |method|

    it "Should send the method '#{method}' to the slave connection" do
      @master_connection.stub!( :open_transactions ).and_return( 0 )
      @slave_connection.should_receive( method ).and_return( true )
      ActiveRecord::Base.connection.send( method )
    end

    it "Should send the method '#{method}' to the master connection if with_master was specified" do
      @master_connection.should_receive( method ).and_return( true )
      ActiveRecord::Base.with_master do
        ActiveRecord::Base.connection.send( method )
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