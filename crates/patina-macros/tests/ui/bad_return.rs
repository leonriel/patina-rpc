use patina_macros::service;

#[service]
trait Bad {
    async fn wrong(&self) -> i32;
}

fn main() {}
