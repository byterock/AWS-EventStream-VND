package AWS::EventStream::VND;

use Digest::Crc32;
use IO::Scalar;
use Carp 'croak';
our $VERSION                 = '0.01';
our $PRELUDE_LENGTH          = 12;
our $PRELUDE_CHECKSUM_LENGTH = 4;
our $OVERHEAD_LENGTH         = 16;

sub decode {
    my ( $io_stream, $output ) = @_;

    croak "You need to provide an IO::Handle to Decode."
      unless ( $io_stream->isa("IO::Handle") );

    croak "You need to provide an Array Ref for the output."
      unless ( ref($output) ne 'ARRAY' );

    while ( !$io_stream->eof ) {
        my $pos = $io_stream->getpos();
        my ( $total_length, $header_length, $prelude_buffer ) =
          _prelude_check($io_stream);
        $io_stream->setpos($pos);
        my ($message_buffer) =
          $self->message_check( $io_stream, $total_length, $header_length );
        my $sh         = new IO::Scalar( \$message_buffer );
        my $headers    = $self->get_headers( $sh, $header_length );
        my $bytes_read = read $sh, my $message, $total_length - 4;
        push( @{$output}, $message );

    }

}

sub get_headers {
    my ( $io_stream, $header_length ) = @_;

    my ($bytes_read) = read $io_stream, my $header_buffer, $header_length;

    my $return_header    = {};
    my $json_decode_type = {
        '0' => {
            pattern => 1,
            length  => 0,
            type    => 'bool_true'
        },
        '1' => {
            pattern => 0,
            length  => 0,
            type    => 'bool_false'
        },
        '2' => {
            pattern => "c",
            length  => 1,
            type    => 'byte'
        },
        '3' => {
            pattern => "s>",
            length  => 2,
            type    => 'short'
        },
        '4' => {
            pattern => "l>",
            length  => 4,
            type    => 'integer'
        },
        '5' => {
            pattern => "q>",
            length  => 8,
            type    => 'long'
        },
        '6' => {
            pattern => undef,
            length  => undef,
            type    => 'bytes'
        },
        '7' => {
            pattern => undef,
            length  => undef,
            type    => 'string'
        },
        '8' => {
            pattern => "q>",
            length  => 8,
            type    => 'timestamp'
        },
        '8' => {
            pattern => undef,
            length  => 16,
            type    => 'uuid'
        },
    };
    my $sh = new IO::Scalar( \$header_buffer );
    
    while ( !$sh->eof ) {
        my $pos = $sh->getpos;
        $bytes_read = read $sh, my $key_len_b, 1;    #get the first byte;
        my $key_len = unpack 'C', $key_len_b;
        ($bytes_read) = read $sh, my $key, $key_len;
        $bytes_read = read $sh, my $type_b, 1;
        my $type = unpack 'C', $type_b;
        my $value_type = $json_decode_type->{$type};
        my $value;

        if (   $value_type->{type} eq 'bool_true'
            || $value_type->{type} eq 'bool_false' )
        {
            $value = $value_type->{pattern};
        }
        else {
            my $value_length = $value_type->{length};
            unless ($value_length) {
                $bytes_read = read $sh, my $value_length_b, 2;
                $value_length = unpack 'S>', $value_length_b;
            }
            ($bytes_read) = read $sh, my $value_b, $value_length;

            if ( $value_type->{pattern} ) {
                $value = unpack $value_type->{pattern}, $value_b;
            }
            else {
                $value = $value_b;
            }
        }

        $return_header->{$key} = $value;
        $pos = $sh->getpos;

    }

    return $return_header;
}

sub message_check {
    my ( $io_stream, $total_length, $header_length ) = @_;
    my $context_length = $OVERHEAD_LENGTH;

    my $bytes_read = read $io_stream, my $buffer, $total_length - 4;

    my $sh = new IO::Scalar( \$buffer );

    $bytes_read = read $sh, my $prelude, $self->$PRELUDE_LENGTH;
    $bytes_read = read $sh, my $message, $total_length - $self->$PRELUDE_LENGTH;
    $bytes_read = read $io_stream, my $messge_checksum, 4;

    my ($checksum) = unpack 'N', $messge_checksum;
    my $crc = new Digest::Crc32();

    if ( $crc->strcrc32($buffer) != $checksum ) {
        die "Message checkum fails!";
    }
    return ($message);
}

sub _prelude_check {
    my ($io_stream) = @_;
    my $bytes_read = read $io_stream, my $prelude_crc, $PRELUDE_LENGTH;

    my $sh = new IO::Scalar( \$prelude_crc );
    $bytes_read = read $sh, my $prelude,
      $PRELUDE_LENGTH - $PRELUDE_CHECKSUM_LENGTH;
    $bytes_read = read $sh, my $prelude_checksum, $PRELUDE_LENGTH;

    my ($checksum) = unpack 'N', $prelude_checksum;

    my $crc = new Digest::Crc32();

    if ( $crc->strcrc32($prelude) != $checksum ) {

        die "Prelude checkum fails!";

    }
    my ( $total_length, $header_length ) = unpack 'N*', $prelude;

    return ( $total_length, $header_length, $sh );

}

1;
