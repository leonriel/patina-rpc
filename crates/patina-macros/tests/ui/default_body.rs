use patina_macros::service;

#[service]
trait Bad {
    async fn has_body(&self) -> Result<(), PatinaError> {
        Ok(())
    }
}

fn main() {}
