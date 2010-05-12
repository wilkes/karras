Vagrant::Config.run do |config|
  config.vm.box = "clojure-dev"

  config.vm.provisioner = :chef_solo
  config.chef.cookbooks_path = "cookbooks"

  config.vm.customize do |vm|
    vm.memory_size = 1024
    vm.name = "Karras VM"
  end

end
