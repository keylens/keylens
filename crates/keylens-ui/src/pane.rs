//! Pane loading state.
//!
//! The important variant is [`PaneState::Unavailable`]. On Upstash, ElastiCache and
//! MemoryDB, half these panes are blocked by the host -- that is the *normal* path, not an
//! error. Modelling it explicitly is what lets the UI say "SLOWLOG is disabled on this
//! server" instead of flashing a red toast the user can't act on.

#[derive(Debug, Clone, PartialEq)]
pub enum PaneState<T> {
    /// Not requested yet. Panes load when first opened, not at connect time.
    Idle,
    Loading,
    Ready(T),
    /// The server refuses or does not implement the backing command.
    Unavailable(String),
    /// A genuine failure, which is worth showing as an error.
    Failed(String),
}

// Written by hand rather than derived: `#[derive(Default)]` on a generic enum adds a
// `T: Default` bound, and none of the pane payloads should have to satisfy that.
#[allow(clippy::derivable_impls)]
impl<T> Default for PaneState<T> {
    fn default() -> Self {
        PaneState::Idle
    }
}

impl<T> PaneState<T> {
    pub fn is_idle(&self) -> bool {
        matches!(self, PaneState::Idle)
    }

    pub fn ready(&self) -> Option<&T> {
        match self {
            PaneState::Ready(v) => Some(v),
            _ => None,
        }
    }

    /// The loaded value, or the message to render in its place.
    ///
    /// **One exhaustive match, so the two cannot drift apart.** Every caller used to ask
    /// `placeholder()` first and then `ready().expect("checked above")`, which is six
    /// separate assertions that those two functions are exact complements — a relationship
    /// nothing enforced. Adding a variant that rendered no placeholder would have turned
    /// all six into panics, in the render path, on a server that merely answered oddly.
    pub fn value_or_message(&self) -> std::result::Result<&T, String> {
        match self {
            PaneState::Ready(v) => Ok(v),
            PaneState::Idle => Err("press r to load".into()),
            PaneState::Loading => Err("loading...".into()),
            PaneState::Unavailable(why) => Err(format!("unavailable on this server - {why}")),
            PaneState::Failed(e) => Err(format!("failed: {e}")),
        }
    }

    /// Message to render when there is nothing to show, or `None` when there is.
    pub fn placeholder(&self) -> Option<String> {
        self.value_or_message().err()
    }

    pub fn is_error(&self) -> bool {
        matches!(self, PaneState::Failed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_is_not_an_error() {
        // A blocked command on a managed host must render as an explanation, not as a
        // failure the user is expected to do something about.
        let state: PaneState<Vec<u8>> = PaneState::Unavailable("NOPERM".into());
        assert!(!state.is_error());
        assert!(
            state
                .placeholder()
                .unwrap()
                .contains("unavailable on this server")
        );
    }

    #[test]
    fn ready_has_no_placeholder() {
        let state = PaneState::Ready(vec![1u8]);
        assert!(state.placeholder().is_none());
        assert_eq!(state.ready(), Some(&vec![1u8]));
    }

    #[test]
    fn failures_surface_the_message() {
        let state: PaneState<()> = PaneState::Failed("connection reset".into());
        assert!(state.is_error());
        assert!(state.placeholder().unwrap().contains("connection reset"));
    }

    #[test]
    fn panes_start_idle_so_they_load_on_first_open() {
        let state: PaneState<()> = PaneState::default();
        assert!(state.is_idle());
    }
}
