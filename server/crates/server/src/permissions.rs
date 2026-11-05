//! Who may do what in a room. Pure functions of the caller's membership, so
//! every rule is unit-tested here and every handler asks the same question
//! the same way.

use protocol::api::{MemberRole, RoomKind};
use storage::rooms::Membership;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Read history, sync, see members.
    Read,
    /// Send messages, typing indicators.
    Post,
    /// Create an invite link.
    Invite,
    /// Remove another member.
    RemoveMember,
    DeleteRoom,
    Leave,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    /// Not a member. Handlers answer 404, so private rooms can't be probed
    /// for existence.
    NotMember,
    /// A member, but not allowed to do this.
    NotAllowed(&'static str),
}

/// Checks an action in a room the caller may or may not belong to.
pub fn check(
    action: Action,
    membership: Option<&Membership>,
    is_guest: bool,
) -> Result<(), Denied> {
    let Some(m) = membership else {
        return Err(Denied::NotMember);
    };
    let owner = m.role == MemberRole::Owner;
    match action {
        Action::Read | Action::Post => Ok(()),
        Action::Invite => {
            if m.kind != RoomKind::Private {
                Err(Denied::NotAllowed("only private rooms have invites"))
            } else if !owner {
                Err(Denied::NotAllowed("only the owner can invite"))
            } else if is_guest {
                Err(Denied::NotAllowed("guests can't invite"))
            } else {
                Ok(())
            }
        }
        Action::RemoveMember | Action::DeleteRoom => {
            if m.kind == RoomKind::Dm {
                Err(Denied::NotAllowed("direct messages have no owner"))
            } else if !owner {
                Err(Denied::NotAllowed("only the owner can do that"))
            } else {
                Ok(())
            }
        }
        Action::Leave => {
            if m.kind == RoomKind::Dm {
                Err(Denied::NotAllowed("direct messages can't be left"))
            } else if owner {
                Err(Denied::NotAllowed(
                    "the owner can't leave; delete the room instead",
                ))
            } else {
                Ok(())
            }
        }
    }
}

/// Room creation, joining public rooms and accepting invites, which happen
/// before there is a membership.
pub fn can_create_room(is_guest: bool) -> Result<(), Denied> {
    if is_guest {
        Err(Denied::NotAllowed("guests can't create rooms"))
    } else {
        Ok(())
    }
}

pub fn can_accept_invite(is_guest: bool) -> Result<(), Denied> {
    if is_guest {
        Err(Denied::NotAllowed("guests can't join private rooms"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(kind: RoomKind, role: MemberRole) -> Membership {
        Membership {
            role,
            kind,
            message_expiry_at: None,
            history_floor: 0,
        }
    }

    const ALL: [Action; 6] = [
        Action::Read,
        Action::Post,
        Action::Invite,
        Action::RemoveMember,
        Action::DeleteRoom,
        Action::Leave,
    ];

    #[test]
    fn non_members_can_do_nothing() {
        for action in ALL {
            assert_eq!(
                check(action, None, false),
                Err(Denied::NotMember),
                "{action:?}"
            );
        }
    }

    /// The whole rule table: (kind, role, guest) -> which actions pass.
    #[test]
    fn rule_table() {
        use Action::*;
        use MemberRole::*;
        use RoomKind::*;
        let cases: &[(RoomKind, MemberRole, bool, &[Action])] = &[
            (
                Public,
                Owner,
                false,
                &[Read, Post, RemoveMember, DeleteRoom],
            ),
            (Public, Member, false, &[Read, Post, Leave]),
            (Public, Member, true, &[Read, Post, Leave]),
            (
                Private,
                Owner,
                false,
                &[Read, Post, Invite, RemoveMember, DeleteRoom],
            ),
            (Private, Member, false, &[Read, Post, Leave]),
            (Dm, Member, false, &[Read, Post]),
            (Dm, Member, true, &[Read, Post]),
        ];
        for &(kind, role, guest, allowed) in cases {
            let m = member(kind, role);
            for action in ALL {
                let expected = allowed.contains(&action);
                assert_eq!(
                    check(action, Some(&m), guest).is_ok(),
                    expected,
                    "{action:?} by {role:?} (guest: {guest}) in {kind:?}"
                );
            }
        }
    }

    #[test]
    fn guests_cant_create_rooms_or_accept_invites() {
        assert!(can_create_room(true).is_err());
        assert!(can_create_room(false).is_ok());
        assert!(can_accept_invite(true).is_err());
        assert!(can_accept_invite(false).is_ok());
    }
}
