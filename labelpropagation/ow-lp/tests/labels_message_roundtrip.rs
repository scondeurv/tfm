use actions::LabelsMessage;
use bytes::Bytes;

#[test]
fn labels_message_roundtrips_through_bytes() {
    let message = LabelsMessage(vec![0, 1, 42, 7_654_321, u32::MAX - 1]);
    let bytes: Bytes = message.clone().into();
    let restored = LabelsMessage::from(bytes);
    assert_eq!(restored, message);
}

#[test]
fn empty_labels_message_roundtrips_through_bytes() {
    let message = LabelsMessage(Vec::new());
    let bytes: Bytes = message.clone().into();
    let restored = LabelsMessage::from(bytes);
    assert_eq!(restored, message);
}
