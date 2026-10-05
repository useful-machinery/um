// Export presentation is independent of the terminal presentation used by
// workflow commands. Authors may resolve these two fields from different Text
// producers after the producing nodes settle.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Field {
    Title,
    Description,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContentReason {
    Blank,
    ControlCharacter,
    TooLarge,
    MultilineTitle,
}

pub(crate) fn resolve_text(field: Field, original: &str) -> Result<&str, ContentReason> {
    let value = original.trim_matches([' ', '\t', '\r', '\n']);
    if value.is_empty() {
        return Err(ContentReason::Blank);
    }
    let mut multiline_title = false;
    for character in value.chars() {
        if field == Field::Title && matches!(character, '\n' | '\u{2028}' | '\u{2029}') {
            multiline_title = true;
            continue;
        }
        if (character < '\u{20}'
            && (character != '\t' || field == Field::Title)
            && (character != '\n' || field == Field::Title))
            || ('\u{7f}'..='\u{9f}').contains(&character)
            || matches!(
                character,
                '\u{2028}' | '\u{2029}' | '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{feff}'
            )
            || ('\u{202a}'..='\u{202e}').contains(&character)
            || ('\u{2066}'..='\u{2069}').contains(&character)
        {
            return Err(ContentReason::ControlCharacter);
        }
    }
    if (field == Field::Title && value.chars().count() > 255)
        || (field == Field::Description && value.len() > 8192)
    {
        return Err(ContentReason::TooLarge);
    }
    if multiline_title {
        return Err(ContentReason::MultilineTitle);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_author_text_with_content_precedence() {
        let examples = [
            (Field::Title, " \t\nProposal\r ", Ok("Proposal")),
            (
                Field::Description,
                "\u{a0}text\u{a0}",
                Ok("\u{a0}text\u{a0}"),
            ),
            (Field::Title, "  \r\n", Err(ContentReason::Blank)),
            (Field::Description, "one\tline\nmore", Ok("one\tline\nmore")),
            (Field::Title, "one\ntwo", Err(ContentReason::MultilineTitle)),
            (
                Field::Title,
                "one\u{2028}two",
                Err(ContentReason::MultilineTitle),
            ),
            (
                Field::Description,
                "one\u{2029}two",
                Err(ContentReason::ControlCharacter),
            ),
            (
                Field::Title,
                "one\t\ntwo",
                Err(ContentReason::ControlCharacter),
            ),
            (
                Field::Description,
                "one\0two",
                Err(ContentReason::ControlCharacter),
            ),
            (
                Field::Description,
                "one\rtwo",
                Err(ContentReason::ControlCharacter),
            ),
            (
                Field::Description,
                "one\u{9f}two",
                Err(ContentReason::ControlCharacter),
            ),
            (
                Field::Title,
                "one\u{2066}two",
                Err(ContentReason::ControlCharacter),
            ),
            (
                Field::Description,
                "one\u{feff}two",
                Err(ContentReason::ControlCharacter),
            ),
        ];
        for (field, original, expected) in examples {
            assert_eq!(resolve_text(field, original), expected);
        }
        let title = "😀".repeat(255);
        assert_eq!(resolve_text(Field::Title, &title), Ok(title.as_str()));
        assert_eq!(
            resolve_text(Field::Title, &("x".repeat(256) + "\nmore")),
            Err(ContentReason::TooLarge)
        );
        let description = "é".repeat(4096);
        assert_eq!(
            resolve_text(Field::Description, &description),
            Ok(description.as_str())
        );
        assert_eq!(
            resolve_text(Field::Description, &"x".repeat(8193)),
            Err(ContentReason::TooLarge)
        );
    }
}
