extern crate burst;
use burst::{BurstBuilder, Machine, MachineSetup};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut b = BurstBuilder::default();
    b.add_set(
        String::from("server"),
        1,
        MachineSetup::new(
            "t3.micro".to_string(),
            "ami-0b46816ffa1234887".to_string(),
            |ssh| {
                Ok(())
                // ssh.exec("sudo yum install htop")
            },
        ),
    );

    // b.add_set(
    //     String::from("client"),
    //     2,
    //     MachineSetup::new("t2.micro".to_string(), "ami-e18aa89b".to_string(), |ssh| {
    //         Ok(())
    //         // ssh.exec("sudo yum install htop");
    //     }),
    // );

    b.run(|vms: HashMap<String, &mut [Machine]>| {
        // let server_ip = vms["server"][0].ip;
        // let cmd = format!("ping{}",server_ip);
        // vms["client"].for_each_parallel(|client| {

        //     client.exec(cmd);
        // })
        Ok(())
    })
    .await
    .expect_err("Failed to run burst");
}
